package com.onthegomap.planetiler.collection;

import com.onthegomap.planetiler.config.PlanetilerConfig;
import com.onthegomap.planetiler.stats.ProcessInfo;
import com.onthegomap.planetiler.stats.ProgressLoggers;
import com.onthegomap.planetiler.stats.Stats;
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
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.concurrent.Semaphore;
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
 * Writes append features to a "chunk" file that can be sorted with 1GB or RAM until it is full, then starts writing to
 * a new chunk. The sort process sorts the chunks, limiting the number of parallel threads by CPU cores and available
 * RAM. Reads do a k-way merge of the sorted chunks using a priority queue of minimum values from each.
 * <p>
 * Only supports single-threaded writes and reads.
 */
@NotThreadSafe
class ExternalMergeSort implements FeatureSort {

  private static final Logger LOGGER = LoggerFactory.getLogger(FeatureSort.class);
  private static final long MAX_CHUNK_SIZE = 1_000_000_000; // 1GB
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
  private Chunk currentChunk;
  private volatile boolean sorted = false;

  ExternalMergeSort(Path tempDir, PlanetilerConfig config, Stats stats) {
    this(
      tempDir,
      config.threads(),
      (int) Math.min(
        MAX_CHUNK_SIZE,
        (ProcessInfo.getMaxMemoryBytes() / 2) / config.threads()
      ),
      config.gzipTempStorage(),
      config,
      stats
    );
  }

  ExternalMergeSort(Path dir, int workers, int chunkSizeLimit, boolean gzip, PlanetilerConfig config, Stats stats) {
    this.config = config;
    this.dir = dir;
    this.stats = stats;
    this.chunkSizeLimit = chunkSizeLimit;
    this.gzip = gzip;
    long memory = ProcessInfo.getMaxMemoryBytes();
    if (chunkSizeLimit > memory / 2) {
      throw new IllegalStateException(
        "Not enough memory to use chunk size " + chunkSizeLimit + " only have " + memory);
    }
    this.workers = workers;
    this.readerLimit = Math.max(1, config.sortMaxReaders());
    this.writerLimit = Math.max(1, config.sortMaxWriters());
    LOGGER.info("Using merge sort feature map, chunk size=" + (chunkSizeLimit / 1_000_000) + "mb workers=" + workers);
    try {
      FileUtils.deleteDirectory(dir);
      Files.createDirectories(dir);
      newChunk();
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
  }

  private static <T> T time(AtomicLong timer, Supplier<T> func) {
    long start = System.nanoTime();
    try {
      return func.get();
    } finally {
      timer.addAndGet(System.nanoTime() - start);
    }
  }

  private DataInputStream newInputStream(Path path) throws IOException {
    InputStream inputStream = new BufferedInputStream(Files.newInputStream(path), 50_000);
    if (gzip) {
      inputStream = new GZIPInputStream(inputStream);
    }
    return new DataInputStream(inputStream);
  }

  private DataOutputStream newOutputStream(Path path) throws IOException {
    OutputStream outputStream = new BufferedOutputStream(Files.newOutputStream(path), 50_000);
    if (gzip) {
      outputStream = new FastGzipOutputStream(outputStream);
    }
    return new DataOutputStream(outputStream);
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
      throw new IllegalStateException(e);
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
          throw new RuntimeException(e);
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
    LOGGER.info("read:" + Duration.ofNanos(reading.get()).toSeconds() +
      "s write:" + Duration.ofNanos(writing.get()).toSeconds() +
      "s sort:" + Duration.ofNanos(sorting.get()).toSeconds() + "s");
  }

  @Override
  public long numFeaturesWritten() {
    return features.get();
  }

  @Override
  public Iterator<SortableFeature> iterator() {
    assert sorted;

    // k-way merge to interleave all the sorted chunks
    PriorityQueue<ChunkIterator> queue = new PriorityQueue<>(chunks.size());
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
        ChunkIterator iterator = queue.poll();
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

  /** Compresses bytes with minimal impact on write performance. Equivalent to {@code gzip -1} */
  private static class FastGzipOutputStream extends GZIPOutputStream {

    public FastGzipOutputStream(OutputStream out) throws IOException {
      super(out);
      def.setLevel(Deflater.BEST_SPEED);
    }
  }

  /**
   * An output segment that can be sorted in ~1GB RAM.
   */
  private class Chunk implements Closeable {

    private final Path path;
    private final DataOutputStream outputStream;
    // estimate how much RAM it would take to sort this chunk
    private int bytesInMemory = 0;
    private int itemCount = 0;

    private Chunk(Path path) throws IOException {
      this.path = path;
      this.outputStream = newOutputStream(path);
    }

    public ChunkIterator newReader() {
      return new ChunkIterator(path, itemCount);
    }

    public void add(SortableFeature entry) throws IOException {
      write(outputStream, entry);
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
      try (ChunkIterator iterator = newReader()) {
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

    private static void write(DataOutputStream out, SortableFeature entry) throws IOException {
      // feature header
      out.writeLong(entry.key());
      out.writeInt(entry.value().length);
      // value
      out.write(entry.value());
    }

    @Override
    public void close() throws IOException {
      outputStream.close();
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
        Arrays.sort(featuresToSort);
        return this;
      }

      public SortableChunk flush() {
        try (DataOutputStream out = newOutputStream(path)) {
          for (SortableFeature feature : featuresToSort) {
            write(out, feature);
          }
          featuresToSort = null;
          return this;
        } catch (IOException e) {
          throw new IllegalStateException(e);
        }
      }
    }
  }

  /**
   * Iterator through all features of a sorted chunk that peeks at the next item before returning it to support k-way
   * merge using a {@link PriorityQueue}.
   */
  private class ChunkIterator implements Closeable, Comparable<ChunkIterator>, Iterator<SortableFeature> {

    private final int count;
    private final DataInputStream input;
    private int read = 0;
    private SortableFeature next;

    ChunkIterator(Path path, int count) {
      this.count = count;
      try {
        input = newInputStream(path);
        next = readNextFeature();
      } catch (IOException e) {
        throw new IllegalStateException(e);
      }
    }

    @Override
    public boolean hasNext() {
      return next != null;
    }

    @Override
    public SortableFeature next() {
      SortableFeature current = next;
      if ((next = readNextFeature()) == null) {
        close();
      }
      return current;
    }

    private SortableFeature readNextFeature() {
      if (read < count) {
        try {
          long nextSort = input.readLong();
          int length = input.readInt();
          byte[] bytes = input.readNBytes(length);
          read++;
          return new SortableFeature(nextSort, bytes);
        } catch (IOException e) {
          throw new IllegalStateException(e);
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

    @Override
    public int compareTo(ChunkIterator o) {
      return next.compareTo(o.next);
    }
  }
}
