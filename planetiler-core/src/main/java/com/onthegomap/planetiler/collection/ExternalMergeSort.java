package com.onthegomap.planetiler.collection;

import static com.onthegomap.planetiler.util.Exceptions.throwFatalException;

import com.onthegomap.planetiler.config.PlanetilerConfig;
import com.onthegomap.planetiler.stats.ProcessInfo;
import com.onthegomap.planetiler.stats.ProgressLoggers;
import com.onthegomap.planetiler.stats.Stats;
import com.onthegomap.planetiler.stats.Timer;
import com.onthegomap.planetiler.util.BinPack;
import com.onthegomap.planetiler.util.ByteBufferUtil;
import com.onthegomap.planetiler.util.CloseableConsumer;
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
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import javax.annotation.concurrent.NotThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xerial.snappy.SnappyInputStream;
import org.xerial.snappy.SnappyOutputStream;

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
  private final List<Chunk> chunks = new CopyOnWriteArrayList<>();
  private final AtomicInteger chunkNum = new AtomicInteger(0);
  private final boolean compress;
  private final PlanetilerConfig config;
  private final int readerLimit;
  private final int writerLimit;
  private final boolean mmapIO;
  private final boolean parallelSort;
  private final boolean madvise;
  private final AtomicBoolean madviseFailed = new AtomicBoolean(false);
  private volatile boolean sorted = false;

  ExternalMergeSort(Path tempDir, PlanetilerConfig config, Stats stats) {
    this(
      tempDir,
      config.threads(),
      (int) Math.min(
        MAX_CHUNK_SIZE,
        ProcessInfo.getMaxMemoryBytes() / 3
      ),
      config.compressTempStorage(),
      config.mmapTempStorage(),
      true,
      true,
      config,
      stats
    );
  }

  ExternalMergeSort(Path dir, int workers, int chunkSizeLimit, boolean compress, boolean mmap, boolean parallelSort,
    boolean madvise, PlanetilerConfig config, Stats stats) {
    this.config = config;
    this.madvise = madvise;
    this.dir = dir;
    this.stats = stats;
    this.parallelSort = parallelSort;
    this.chunkSizeLimit = chunkSizeLimit;
    if (compress && mmap) {
      LOGGER.warn("--compress-temp option not supported with --mmap-temp, falling back to --mmap-temp=false");
      mmap = false;
    }
    this.compress = compress;
    this.mmapIO = mmap;
    long memLimit = ProcessInfo.getMaxMemoryBytes() / 3;
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
  public CloseableConsumer<SortableFeature> writerForThread() {
    return new ThreadLocalWriter();
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
    for (var chunk : chunks) {
      try {
        chunk.close();
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

    // we may end up with many small chunks because each thread-local writer starts a new one
    // so group together smaller chunks that can be sorted together in-memory to minimize the
    // number of chunks that the reader needs to deal with
    List<List<ExternalMergeSort.Chunk>> groups = BinPack.pack(
      chunks,
      chunkSizeLimit,
      chunk -> chunk.bytesInMemory
    );

    LOGGER.info("Grouped {} chunks into {}", chunks.size(), groups.size());

    var pipeline = WorkerPipeline.start("sort", stats)
      .readFromTiny("item_queue", groups)
      .sinkToConsumer("worker", workers, group -> {
        try {
          readSemaphore.acquire();
          var chunk = group.getFirst();
          var others = group.stream().skip(1).toList();
          var toSort = time(reading, () -> {
            // merge all chunks into first one, and remove the others
            var result = chunk.readAllAndMergeIn(others);
            for (var other : others) {
              other.remove();
            }
            return result;
          });
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
      .addPercentCounter("chunks", groups.size(), doneCounter)
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
  public Iterator<SortableFeature> iterator(int shard, int shards) {
    assert sorted;
    if (shard < 0 || shard >= shards) {
      throw new IllegalArgumentException("Bad shard params: shard=%d shards=%d".formatted(shard, shards));
    }

    if (chunks.isEmpty()) {
      return Collections.emptyIterator();
    }

    // k-way merge to interleave all the sorted chunks
    List<Reader> iterators = new ArrayList<>();
    for (int i = shard; i < chunks.size(); i += shards) {
      var chunk = chunks.get(i);
      if (chunk.itemCount > 0) {
        iterators.add(chunk.newReader());
      }
    }

    return LongMerger.mergeIterators(iterators, SortableFeature.COMPARE_BYTES);
  }

  @Override
  public int chunksToRead() {
    return chunks.size();
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

  private interface Reader extends Closeable, Iterator<SortableFeature> {

    @Override
    void close();
  }

  /** Read all features from a chunk file using a {@link BufferedInputStream}. */
  private static class ReaderBuffered extends BaseReader {

    private final int count;
    private final DataInputStream input;
    private int read = 0;

    ReaderBuffered(Path path, int count, boolean compress) {
      this.count = count;
      try {
        InputStream inputStream = new BufferedInputStream(Files.newInputStream(path));
        if (compress) {
          inputStream = new SnappyInputStream(inputStream);
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

    WriterBuffered(Path path, boolean compress) {
      try {
        OutputStream rawOutputStream = new BufferedOutputStream(Files.newOutputStream(path));
        if (compress) {
          rawOutputStream = new SnappyOutputStream(rawOutputStream);
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
  private abstract static class BaseReader implements Reader {

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

    abstract SortableFeature readNextFeature();
  }

  /** Writer that a single thread can use to write features independent of writers used in other threads. */
  @NotThreadSafe
  private class ThreadLocalWriter implements CloseableConsumer<SortableFeature> {

    private Chunk currentChunk;

    private ThreadLocalWriter() {
      try {
        newChunk();
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }

    @Override
    public void accept(SortableFeature item) {
      assert !sorted;
      try {
        features.incrementAndGet();
        currentChunk.add(item);
        if (currentChunk.bytesInMemory > chunkSizeLimit) {
          newChunk();
        }
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }

    private void newChunk() throws IOException {
      Path chunkPath = dir.resolve("chunk" + chunkNum.incrementAndGet());
      FileUtils.deleteOnExit(chunkPath);
      if (currentChunk != null) {
        currentChunk.close();
      }
      chunks.add(currentChunk = new Chunk(chunkPath));
    }

    @Override
    public void close() throws IOException {
      if (currentChunk != null) {
        currentChunk.close();
      }
    }
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

    private SortableChunk readAllAndMergeIn(Collection<Chunk> others) {
      // first, grow this chunk
      int newItems = itemCount;
      int newBytes = bytesInMemory;
      for (var other : others) {
        if (Integer.MAX_VALUE - newItems < other.itemCount) {
          throw new IllegalStateException("Too many items in merged chunk: " + itemCount + "+" +
            others.stream().map(c -> c.itemCount).toList());
        }
        if (Integer.MAX_VALUE - newBytes < other.bytesInMemory) {
          throw new IllegalStateException("Too big merged chunk: " + bytesInMemory + "+" +
            others.stream().map(c -> c.bytesInMemory).toList());
        }
        newItems += other.itemCount;
        newBytes += other.bytesInMemory;
      }
      // then read items from all chunks into memory
      SortableChunk result = new SortableChunk(newItems);
      result.readAll(this);
      itemCount = newItems;
      bytesInMemory = newBytes;
      for (var other : others) {
        result.readAll(other);
      }
      if (result.i != itemCount) {
        throw new IllegalStateException("Expected " + itemCount + " features in " + path + " got " + result.i);
      }
      return result;
    }

    private Writer newWriter(Path path) {
      return mmapIO ? new WriterMmap(path) : new WriterBuffered(path, compress);
    }

    private Reader newReader() {
      return mmapIO ? new ReaderMmap(path, itemCount) : new ReaderBuffered(path, itemCount, compress);
    }

    @Override
    public void close() throws IOException {
      writer.close();
    }

    public void remove() {
      chunks.remove(this);
      FileUtils.delete(path);
    }

    /**
     * A container for all features in a chunk read into memory for sorting.
     */
    private class SortableChunk {

      private SortableFeature[] featuresToSort;
      private int i = 0;

      private SortableChunk(int itemCount) {
        this.featuresToSort = new SortableFeature[itemCount];
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

      private void readAll(Chunk chunk) {
        try (var iterator = chunk.newReader()) {
          while (iterator.hasNext()) {
            featuresToSort[i++] = iterator.next();
          }
        }
      }
    }
  }

  /** Memory-map the chunk file, then iterate through all features in it. */
  private class ReaderMmap extends BaseReader {

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
