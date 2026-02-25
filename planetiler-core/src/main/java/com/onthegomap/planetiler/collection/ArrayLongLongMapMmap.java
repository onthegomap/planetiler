package com.onthegomap.planetiler.collection;

import static com.onthegomap.planetiler.util.Exceptions.throwFatalException;
import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;

import com.carrotsearch.hppc.BitSet;
import com.onthegomap.planetiler.stats.ProcessInfo;
import com.onthegomap.planetiler.util.ByteBufferUtil;
import com.onthegomap.planetiler.util.FileUtils;
import com.onthegomap.planetiler.util.SlidingWindow;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A map from sequential {@code long} keys to {@code long} values backed by a file on disk where the key defines the
 * offset in the input file.
 * <p>
 * During write phase, values are stored in a sliding window of {@link ByteBuffer ByteBuffers} and flushed to disk when
 * the segment slides out of the window. During read phase, they file is memory-mapped and read.
 */
class ArrayLongLongMapMmap implements LongLongMap.ParallelWrites {
  /*
   * In order to limit the number of in-memory segments during writes and ensure liveliness, keep track
   * of the current segment index that each worker is working on in the "segments" array. Then use
   * slidingWindow to make threads that try to allocate new segments wait until old segments are
   * finished. Also use activeSegments semaphore to make new segments wait to allocate until
   * old segments are actually flushed to disk.
   *
   * TODO: cleaner way to limit in-memory segments with sliding window that does not also need the semaphore?
   * TODO: extract maintaining segments list into a separate utility?
   */

  // 128MB per chunk
  private static final int DEFAULT_SEGMENT_BITS = 27;
  // work on up to 5GB of data at a time
  private static final long MAX_BYTES_TO_USE = 5_000_000_000L;
  private final boolean madvise;
  private final int segmentBits;
  private final long segmentMask;
  private final long segmentBytes;
  private final SlidingWindow slidingWindow;
  private final Path path;
  private final CopyOnWriteArrayList<AtomicInteger> segments = new CopyOnWriteArrayList<>();
  private final ConcurrentHashMap<Integer, Segment> writeBuffers = new ConcurrentHashMap<>();
  private final Semaphore activeSegments;
  private final BitSet usedSegments = new BitSet();
  private FileChannel writeChannel;
  private MappedByteBuffer[] segmentsArray;
  private FileChannel readChannel = null;
  private volatile int tail = 0;
  private volatile boolean initialized = false;

  ArrayLongLongMapMmap(Path path, boolean madvise) {
    this(
      path,
      DEFAULT_SEGMENT_BITS,
      guessPendingChunkLimit(1L << DEFAULT_SEGMENT_BITS),
      madvise
    );
  }

  ArrayLongLongMapMmap(Path path, int segmentBits, int maxPendingSegments, boolean madvise) {
    if (segmentBits < 3) {
      throw new IllegalArgumentException("Segment size must be a multiple of 8, got 2^" + segmentBits);
    }
    this.activeSegments = new Semaphore(maxPendingSegments);
    this.madvise = madvise;
    this.segmentBits = segmentBits;
    segmentMask = (1L << segmentBits) - 1;
    segmentBytes = 1L << segmentBits;
    slidingWindow = new SlidingWindow(maxPendingSegments);
    this.path = path;
    try {
      writeChannel = FileChannel.open(path, WRITE, CREATE);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private static int guessPendingChunkLimit(long chunkSize) {
    int minChunks = 1;
    int maxChunks = (int) (MAX_BYTES_TO_USE / chunkSize);
    int targetChunks = (int) (ProcessInfo.getMaxMemoryBytes() * 0.5d / chunkSize);
    return Math.clamp(targetChunks, minChunks, maxChunks);
  }

  public void init() {
    try {
      for (var entry : writeBuffers.entrySet()) {
        if (entry.getKey() < Integer.MAX_VALUE) {
          // no one else needs this segment, flush it
          var toFlush = entry.setValue(DONE_SEGMENT);
          if (toFlush != null && toFlush != DONE_SEGMENT) {
            toFlush.flushToDisk();
          }
        }
      }
      writeChannel.close();
      readChannel = FileChannel.open(path, READ);
      segmentsArray = ByteBufferUtil.mapFile(readChannel, readChannel.size(), segmentBytes, madvise, usedSegments::get);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  @Override
  public Writer newWriter() {
    return new Writer();
  }

  private void initOnce() {
    if (!initialized) {
      synchronized (this) {
        if (!initialized) {
          init();
          initialized = true;
        }
      }
    }
  }

  @Override
  public long get(long key) {
    initOnce();
    long byteOffset = key << 3;
    int idx = (int) (byteOffset >>> segmentBits);
    if (idx >= segmentsArray.length) {
      return LongLongMap.MISSING_VALUE;
    }
    MappedByteBuffer mappedByteBuffer = segmentsArray[idx];
    if (mappedByteBuffer == null) {
      return LongLongMap.MISSING_VALUE;
    }
    int offset = (int) (byteOffset & segmentMask);
    long result = mappedByteBuffer.getLong(offset);
    return result == 0 ? LongLongMap.MISSING_VALUE : result;
  }

  @Override
  public long diskUsageBytes() {
    return FileUtils.size(path);
  }

  @Override
  public long estimateMemoryUsageBytes() {
    return 0;
  }

  @Override
  public void close() throws IOException {
    if (segmentsArray != null) {
      ByteBufferUtil.free(segmentsArray);
      segmentsArray = null;
    }
    if (writeChannel != null) {
      writeChannel.close();
      writeChannel = null;
    }
    if (readChannel != null) {
      readChannel.close();
      readChannel = null;
    }
    FileUtils.delete(path);
  }

  /**
   * Instructions that tell a thread which segments must be flushed, and which must be allocated before any threads can
   * start writing to the result segment.
   */
  private static class SegmentActions {
    private final List<Segment> flush = new ArrayList<>();
    private final List<Segment> allocate = new ArrayList<>();
    private Segment result = null;
    private boolean done = false;

    void setResult(Segment result) {
      this.result = result;
    }

    void perform() {
      if (!done) {
        // if this thread is allocating a new segment, then wait on allocating it
        // if this thread is just using one, then wait for it to become available
        flush.forEach(Segment::flushToDisk);
        allocate.forEach(Segment::allocate);
        done = true;
      }
    }

    ByteBuffer awaitBuffer() {
      return result.await();
    }
  }

  private final Segment DONE_SEGMENT = new Segment() {
    @Override
    ByteBuffer await() {
      throw new IllegalArgumentException("await called on closed segment");
    }

    @Override
    void allocate() {
      throw new IllegalArgumentException("allocate called on closed segment");
    }

    @Override
    void flushToDisk() {
      throw new IllegalArgumentException("flushToDisk called on closed segment");
    }

    @Override
    public String toString() {
      return "DONE";
    }
  };

  /**
   * A segment of the storage file that threads can update in parallel, and can be flushed to disk when all threads are
   * done writing to it.
   */
  private class Segment {

    private final int id;
    private final long offset;
    private CompletableFuture<ByteBuffer> result = new CompletableFuture<>();

    private Segment(int id) {
      this.offset = ((long) id) << segmentBits;
      this.id = id;
    }

    public Segment() {
      this(0);
    }

    @Override
    public String toString() {
      return "Segment[" + id + ']';
    }

    ByteBuffer await() {
      try {
        return result.get();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return throwFatalException(e);
      } catch (ExecutionException e) {
        return throwFatalException(e);
      }
    }

    void allocate() {
      slidingWindow.waitUntilInsideWindow(id);
      try {
        activeSegments.acquire();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throwFatalException(e);
      }
      synchronized (usedSegments) {
        usedSegments.set(id);
      }
      result.complete(ByteBuffer.allocate(1 << segmentBits));
    }

    void flushToDisk() {
      try {
        ByteBuffer buffer = result.get();
        writeChannel.write(buffer, offset);
        result = null;
        activeSegments.release();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throwFatalException(e);
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      } catch (ExecutionException e) {
        throwFatalException(e);
      }
    }
  }

  /** Handle for a single worker thread to write values in parallel with other workers. */
  private class Writer implements LongLongMap.Writer {
    final AtomicInteger currentSeg = new AtomicInteger(0);
    long lastSegment = -1;
    long segmentOffset = -1;
    ByteBuffer buffer = null;

    Writer() {
      segments.add(currentSeg);
    }

    @Override
    public void close() {
      SegmentActions actions = advanceTo(Integer.MAX_VALUE);
      actions.perform();
    }

    @Override
    public void put(long key, long value) {
      long offset = key << 3;
      long segment = offset >>> segmentBits;
      // this thread is moving onto the next segment, so coordinate with other threads to allocate
      // a new buffer if necessary while limiting maximum number of segments held in-memory
      if (segment > lastSegment) {
        if (segment >= Integer.MAX_VALUE) {
          throw new IllegalArgumentException("Segment " + segment + " > Integer.MAX_VALUE");
        }
        SegmentActions actions = advanceTo((int) segment);
        // iterate through the tail-end and free up chunks that aren't needed anymore
        actions.perform();

        // wait on adding a new buffer to head until the number of pending buffers is small enough
        buffer = actions.awaitBuffer();
        lastSegment = segment;
        segmentOffset = segment << segmentBits;
      } else if (segment < lastSegment) {
        throw new IllegalStateException("Out-of-order insertions not allowed");
      }
      buffer.putLong((int) (offset - segmentOffset), value);
    }

    private SegmentActions advanceTo(int value) {
      synchronized (ArrayLongLongMapMmap.this) {
        currentSeg.set(value);
        SegmentActions result = new SegmentActions();
        var min = segments.stream().mapToInt(AtomicInteger::get).min().orElseThrow();
        if (min == Integer.MAX_VALUE) {
          // all workers are done, flush everything
          for (var entry : writeBuffers.entrySet()) {
            var old = entry.setValue(DONE_SEGMENT);
            if (old != DONE_SEGMENT) {
              result.flush.add(old);
            }
          }
          tail = min;
        } else if (value == Integer.MAX_VALUE) {
          // this worker is done, advance tail to min
          for (var entry : writeBuffers.entrySet()) {
            if (entry.getKey() < min) {
              var segment = entry.setValue(DONE_SEGMENT);
              if (segment != null && segment != DONE_SEGMENT) {
                result.flush.add(segment);
              }
            }
          }
          tail = min;
        } else {
          // if the tail segment just finished, then advance the tail and flush all pending segments
          while (tail < min) {
            var segment = writeBuffers.put(tail, DONE_SEGMENT);
            if (segment != null && segment != DONE_SEGMENT) {
              result.flush.add(segment);
            }
            tail++;
          }
          Segment segment = writeBuffers.computeIfAbsent(value, id -> {
            var seg = new Segment(id);
            result.allocate.add(seg);
            return seg;
          });
          result.setResult(segment);
        }

        // let workers waiting to allocate new segments to the head of the sliding window proceed
        // NOTE: the memory hasn't been released yet, so the activeChunks semaphore will cause
        // those workers to wait until the memory has been released.
        slidingWindow.advanceTail(tail);
        return result;
      }
    }
  }
}
