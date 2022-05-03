package com.onthegomap.planetiler.collection;

import static com.onthegomap.planetiler.util.Exceptions.throwFatalException;

import com.onthegomap.planetiler.config.PlanetilerConfig;
import com.onthegomap.planetiler.stats.ProcessInfo;
import com.onthegomap.planetiler.stats.ProgressLoggers;
import com.onthegomap.planetiler.stats.Stats;
import com.onthegomap.planetiler.stats.Timer;
import com.onthegomap.planetiler.util.ByteBufferUtil;
import com.onthegomap.planetiler.util.FileUtils;
import com.onthegomap.planetiler.worker.WorkerPipeline;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.PriorityQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import java.util.zip.Deflater;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import javax.annotation.concurrent.NotThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A utility that writes {@link SortableFeature SortableFeatures} to disk and uses merge sort to efficiently sort much
 * more data than fits in RAM.
 * <p>
 * Writes append features to a "chunk" file that can be sorted with a fixed amount of RAM, then starts writing to a new
 * chunk. The sort process sorts the chunks, limiting the number of parallel threads by CPU cores and available RAM.
 * Reads do a k-way merge of the sorted chunks using a priority queue of minimum values from each.
 * <p>
 * Only supports single-threaded writes and reads.
 */
@NotThreadSafe
class ExternalMergeSort implements FeatureSort {

  private static final Logger LOGGER = LoggerFactory.getLogger(ExternalMergeSort.class);
  private static final long MAX_CHUNK_SIZE = 2_000_000_000; // 2GB
  private final Path dir;
  private final Stats stats;
  private final int chunkSizeLimit;
  private final int workers;
  private final AtomicLong features = new AtomicLong(0);
  private final List<Chunk> chunks = new ArrayList<>();
  private final boolean gzip;
  private final PlanetilerConfig config;
  private final int readerLimit;
  private final int writerLimit;
  private final boolean mmapIO;
  private final boolean parallelSort;
  private final boolean madvise;
  private final AtomicBoolean madviseFailed = new AtomicBoolean(false);
  private Chunk currentChunk;
  private volatile boolean sorted = false;

  ExternalMergeSort(Path tempDir, PlanetilerConfig config, Stats stats) {
    this(
      tempDir,
      config.threads(),
      (int) Math.min(
        MAX_CHUNK_SIZE,
        ProcessInfo.getMaxMemoryBytes() / 3
      ),
      config.gzipTempStorage(),
      config.mmapTempStorage(),
      true,
      true,
      config,
      stats
    );
  }

  ExternalMergeSort(Path dir, int workers, int chunkSizeLimit, boolean gzip, boolean mmap, boolean parallelSort,
    boolean madvise, PlanetilerConfig config, Stats stats) {
    this.config = config;
    this.madvise = madvise;
    this.dir = dir;
    this.stats = stats;
    this.parallelSort = parallelSort;
    this.chunkSizeLimit = chunkSizeLimit;
    if (gzip && mmap) {
      LOGGER.warn("--gzip-temp option not supported with --mmap-temp, falling back to --gzip-temp=false");
      gzip = false;
    }
    this.gzip = gzip;
    this.mmapIO = mmap;
    long memLimit = ProcessInfo.getMaxMemoryBytes() / 2;
    if (chunkSizeLimit > memLimit) {
      throw new IllegalStateException("Not enough memory for chunkSize=" + chunkSizeLimit + " limit=" + memLimit);
    }
    int maxWorkersBasedOnMemory = Math.max(1, (int) (memLimit / Math.max(1, chunkSizeLimit)));
    this.workers = Math.min(workers, maxWorkersBasedOnMemory);
    this.readerLimit = Math.max(1, config.sortMaxReaders());
    this.writerLimit = Math.max(1, config.sortMaxWriters());
    LOGGER.info("Using merge sort feature map, chunk size={}mb max workers={}", chunkSizeLimit / 1_000_000, workers);
    try {
      FileUtils.deleteDirectory(dir);
      Files.createDirectories(dir);
      newChunk();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private static <T> T time(AtomicLong total, Supplier<T> func) {
    var timer = Timer.start();
    try {
      return func.get();
    } finally {
      total.addAndGet(timer.stop().elapsed().wall().toNanos());
    }
  }

  @Override
  public void add(SortableFeature item) {
    try {
      assert !sorted;
      features.incrementAndGet();
      currentChunk.add(item);
      if (currentChunk.bytesInMemory > chunkSizeLimit) {
        newChunk();
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  @Override
  public long diskUsageBytes() {
    return FileUtils.directorySize(dir);
  }

  @Override
  public long estimateMemoryUsageBytes() {
    return 0;
  }

  @Override
  public void sort() {
    assert !sorted;
    if (currentChunk != null) {
      try {
        currentChunk.close();
      } catch (IOException e) {
        // ok
      }
    }
    var timer = stats.startStage("sort");
    Semaphore readSemaphore = new Semaphore(readerLimit);
    Semaphore writeSemaphore = new Semaphore(writerLimit);
    AtomicLong reading = new AtomicLong(0);
    AtomicLong writing = new AtomicLong(0);
    AtomicLong sorting = new AtomicLong(0);
    AtomicLong doneCounter = new AtomicLong(0);

    var pipeline = WorkerPipeline.start("sort", stats)
      .readFromTiny("item_queue", chunks)
      .sinkToConsumer("worker", workers, chunk -> {
        try {
          readSemaphore.acquire();
          var toSort = time(reading, chunk::readAll);
          readSemaphore.release();

          time(sorting, toSort::sort);

          writeSemaphore.acquire();
          time(writing, toSort::flush);
          writeSemaphore.release();

          doneCounter.incrementAndGet();
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throwFatalException(e);
        }
      });

    ProgressLoggers loggers = ProgressLoggers.create()
      .addPercentCounter("chunks", chunks.size(), doneCounter)
      .addFileSize(this)
      .newLine()
      .addProcessStats()
      .newLine()
      .addPipelineStats(pipeline);

    pipeline.awaitAndLog(loggers, config.logInterval());

    sorted = true;
    timer.stop();
    LOGGER.info("read:{}s write:{}s sort:{}s",
      Duration.ofNanos(reading.get()).toSeconds(),
      Duration.ofNanos(writing.get()).toSeconds(),
      Duration.ofNanos(sorting.get()).toSeconds());
  }

  @Override
  public long numFeaturesWritten() {
    return features.get();
  }

  @Override
  public Iterator<SortableFeature> iterator() {
    assert sorted;

    // k-way merge to interleave all the sorted chunks
    PriorityQueue<Reader<?>> queue = new PriorityQueue<>(chunks.size());
    for (Chunk chunk : chunks) {
      if (chunk.itemCount > 0) {
        queue.add(chunk.newReader());
      }
    }

    return new Iterator<>() {
      @Override
      public boolean hasNext() {
        return !queue.isEmpty();
      }

      @Override
      public SortableFeature next() {
        Reader<?> iterator = queue.poll();
        assert iterator != null;
        SortableFeature next = iterator.next();
        if (iterator.hasNext()) {
          queue.add(iterator);
        }
        return next;
      }
    };
  }

  private void newChunk() throws IOException {
    Path chunkPath = dir.resolve("chunk" + (chunks.size() + 1));
    chunkPath.toFile().deleteOnExit();
    if (currentChunk != null) {
      currentChunk.close();
    }
    chunks.add(currentChunk = new Chunk(chunkPath));
  }

  public int chunks() {
    return chunks.size();
  }

  private void tryMadviseSequential(ByteBuffer buffer) {
    try {
      ByteBufferUtil.posixMadvise(buffer, ByteBufferUtil.Madvice.SEQUENTIAL);
    } catch (IOException e) {
      if (madviseFailed.compareAndSet(false, true)) { // log once
        LOGGER.info("madvise not available on this system to speed up temporary feature IO.");
      }
    }
  }

  private interface Writer extends Closeable {
    void write(SortableFeature feature) throws IOException;
  }

  private interface Reader<T extends Reader<?>>
    extends Closeable, Iterator<SortableFeature>, Comparable<T> {

    @Override
    void close();
  }

  /** Compresses bytes with minimal impact on write performance. Equivalent to {@code gzip -1} */
  private static class FastGzipOutputStream extends GZIPOutputStream {

    public FastGzipOutputStream(OutputStream out) throws IOException {
      super(out);
      def.setLevel(Deflater.BEST_SPEED);
    }
  }

  /** Read all features from a chunk file using a {@link BufferedInputStream}. */
  private static class ReaderBuffered extends BaseReader<ReaderBuffered> {

    private final int count;
    private final DataInputStream input;
    private int read = 0;

    ReaderBuffered(Path path, int count, boolean gzip) {
      this.count = count;
      try {
        InputStream inputStream = new BufferedInputStream(Files.newInputStream(path));
        if (gzip) {
          inputStream = new GZIPInputStream(inputStream);
        }
        input = new DataInputStream(inputStream);
        next = readNextFeature();
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }

    @Override
    SortableFeature readNextFeature() {
      if (read < count) {
        try {
          long nextSort = input.readLong();
          int length = input.readInt();
          byte[] bytes = input.readNBytes(length);
          read++;
          return new SortableFeature(nextSort, bytes);
        } catch (IOException e) {
          throw new UncheckedIOException(e);
        }
      } else {
        return null;
      }
    }

    @Override
    public void close() {
      try {
        input.close();
      } catch (IOException e) {
        LOGGER.warn("Error closing chunk", e);
      }
    }
  }

  /** Write features to the chunk file using a {@link BufferedOutputStream}. */
  private static class WriterBuffered implements Writer {

    private final DataOutputStream out;

    WriterBuffered(Path path, boolean gzip) {
      try {
        OutputStream rawOutputStream = new BufferedOutputStream(Files.newOutputStream(path));
        if (gzip) {
          rawOutputStream = new FastGzipOutputStream(rawOutputStream);
        }
        this.out = new DataOutputStream(rawOutputStream);
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }

    @Override
    public void close() throws IOException {
      out.close();
    }

    @Override
    public void write(SortableFeature feature) throws IOException {
      out.writeLong(feature.key());
      out.writeInt(feature.value().length);
      out.write(feature.value());
    }
  }

  /** Common functionality between {@link ReaderMmap} and {@link ReaderBuffered}. */
  private abstract static class BaseReader<T extends BaseReader<?>> implements Reader<T> {
    SortableFeature next;

    @Override
    public final boolean hasNext() {
      return next != null;
    }

    @Override
    public final SortableFeature next() {
      SortableFeature current = next;
      if (current == null) {
        throw new NoSuchElementException();
      }
      if ((next = readNextFeature()) == null) {
        close();
      }
      return current;
    }

    @Override
    public final int compareTo(T o) {
      return next.compareTo(o.next);
    }

    abstract SortableFeature readNextFeature();
  }

  /** Write features to the chunk file through a memory-mapped file. */
  private class WriterMmap implements Writer {
    private final FileChannel channel;
    private final MappedByteBuffer buffer;

    WriterMmap(Path path) {
      try {
        this.channel =
          FileChannel.open(path, StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.READ);
        this.buffer = channel.map(FileChannel.MapMode.READ_WRITE, 0, chunkSizeLimit);
        if (madvise) {
          tryMadviseSequential(buffer);
        }
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }

    @Override
    public void close() throws IOException {
      // on windows, truncating throws an exception if the file is still mapped
      ByteBufferUtil.free(buffer);
      channel.truncate(buffer.position());
      channel.close();
    }


    @Override
    public void write(SortableFeature feature) throws IOException {
      buffer.putLong(feature.key());
      buffer.putInt(feature.value().length);
      buffer.put(feature.value());
    }
  }

  /**
   * An output segment that can be sorted with a fixed amount of RAM.
   */
  private class Chunk implements Closeable {

    private final Path path;
    private final Writer writer;
    // estimate how much RAM it would take to sort this chunk
    private int bytesInMemory = 0;
    private int itemCount = 0;

    private Chunk(Path path) {
      this.path = path;
      this.writer = newWriter(path);
    }

    public void add(SortableFeature entry) throws IOException {
      writer.write(entry);
      bytesInMemory +=
        // pointer to feature
        8 +
          // Feature class overhead
          16 +
          // long sort member of feature
          8 +
          // byte array pointer
          8 +
          // byte array size
          24 + entry.value().length;
      itemCount++;
    }

    private SortableChunk readAll() {
      try (var iterator = newReader()) {
        SortableFeature[] featuresToSort = new SortableFeature[itemCount];
        int i = 0;
        while (iterator.hasNext()) {
          featuresToSort[i] = iterator.next();
          i++;
        }
        if (i != itemCount) {
          throw new IllegalStateException("Expected " + itemCount + " features in " + path + " got " + i);
        }
        return new SortableChunk(featuresToSort);
      }
    }

    private Writer newWriter(Path path) {
      return mmapIO ? new WriterMmap(path) : new WriterBuffered(path, gzip);
    }

    private Reader<?> newReader() {
      return mmapIO ? new ReaderMmap(path, itemCount) : new ReaderBuffered(path, itemCount, gzip);
    }

    @Override
    public void close() throws IOException {
      writer.close();
    }

    /**
     * A container for all features in a chunk read into memory for sorting.
     */
    private class SortableChunk {

      private SortableFeature[] featuresToSort;

      private SortableChunk(SortableFeature[] featuresToSort) {
        this.featuresToSort = featuresToSort;
      }

      public SortableChunk sort() {
        if (parallelSort) {
          Arrays.parallelSort(featuresToSort);
        } else {
          Arrays.sort(featuresToSort);
        }
        return this;
      }

      public SortableChunk flush() {
        try (Writer out = newWriter(path)) {
          for (SortableFeature feature : featuresToSort) {
            out.write(feature);
          }
          featuresToSort = null;
          return this;
        } catch (IOException e) {
          throw new UncheckedIOException(e);
        }
      }
    }
  }

  /** Memory-map the chunk file, then iterate through all features in it. */
  private class ReaderMmap extends BaseReader<ReaderMmap> {
    private final int count;
    private final FileChannel channel;
    private final MappedByteBuffer buffer;
    private int read = 0;

    ReaderMmap(Path path, int count) {
      this.count = count;
      try {
        channel = FileChannel.open(path, StandardOpenOption.READ);
        buffer = channel.map(FileChannel.MapMode.READ_ONLY, 0, channel.size());
        if (madvise) {
          // give the OS a hint that pages will be read sequentially so it can read-ahead and drop as soon as we're done
          tryMadviseSequential(buffer);
        }
        next = readNextFeature();
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }

    @Override
    SortableFeature readNextFeature() {
      if (read < count) {
        long nextSort = buffer.getLong();
        int length = buffer.getInt();
        byte[] bytes = new byte[length];
        buffer.get(bytes);
        read++;
        return new SortableFeature(nextSort, bytes);
      } else {
        return null;
      }
    }

    @Override
    public void close() {
      try {
        ByteBufferUtil.free(buffer);
      } catch (IOException e) {
        LOGGER.info("Unable to unmap chunk", e);
      }
      try {
        channel.close();
      } catch (IOException e) {
        LOGGER.warn("Error closing chunk", e);
      }
    }
  }
}
