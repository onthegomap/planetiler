package com.onthegomap.planetiler.collection;

import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;

import com.carrotsearch.hppc.BitSet;
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
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ArrayLongLongMapMmap implements LongLongMap.ParallelWrites {

  private static final Logger LOGGER = LoggerFactory.getLogger(ArrayLongLongMapMmap.class);

  FileChannel writeChannel;
  private final int segmentBits;
  private final long segmentMask;
  private final long segmentBytes;
  private final SlidingWindow slidingWindow;
  private final Path path;
  private MappedByteBuffer[] segmentsArray;
  private final CopyOnWriteArrayList<AtomicInteger> segments = new CopyOnWriteArrayList<>();
  private final ConcurrentHashMap<Integer, Segment> writeBuffers = new ConcurrentHashMap<>();
  private FileChannel readChannel = null;
  private volatile int tail = 0;
  private final BitSet usedSegments = new BitSet();

  public ArrayLongLongMapMmap(Path path) {
    this(
      path,
      27, // 128MB per chunk
      20 // 2.5GB of pending chunks
    );
  }

  public ArrayLongLongMapMmap(Path path, int segmentBits, int maxPendingSegments) {
    if (segmentBits < 3) {
      throw new IllegalArgumentException("Segment size must be a multiple of 8, got 2^" + segmentBits);
    }
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

  public void init() {
    try {
      log("init: " + writeBuffers);
      for (Integer oldKey : writeBuffers.keySet()) {
        if (oldKey < Integer.MAX_VALUE) {
          // no one else needs this segment, flush it
          var toFlush = writeBuffers.remove(oldKey);
          if (toFlush != null) {
            toFlush.flush();
          }
        }
      }
      writeChannel.close();
      readChannel = FileChannel.open(path, READ, WRITE);
      long outIdx = readChannel.size() + 8;
      int segmentCount = (int) (outIdx / segmentBytes + 1);
      segmentsArray = new MappedByteBuffer[segmentCount];
      int i = 0;
      for (long segmentStart = 0; segmentStart < outIdx; segmentStart += segmentBytes) {
        long segmentLength = Math.min(segmentBytes, outIdx - segmentStart);
        // TODO madvise
        if (usedSegments.get(i)) {
          MappedByteBuffer buffer = readChannel.map(FileChannel.MapMode.READ_ONLY, segmentStart, segmentLength);
          log("  mapping from " + segmentStart + "->" + (segmentStart + segmentLength));
          segmentsArray[i] = buffer;
        }
        i++;
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private class Segment {

    private final int id;
    private final long offset;
    private CompletableFuture<ByteBuffer> result = new CompletableFuture<>();

    private Segment(int id) {
      this.offset = ((long) id) << segmentBits;
      this.id = id;
    }

    public int id() {
      return id;
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
        throw new RuntimeException(e);
      } catch (ExecutionException e) {
        throw new RuntimeException(e);
      }
    }

    void allocate() {
      slidingWindow.waitUntilInsideWindow(id);
      log("  allocating " + id);
      result.complete(ByteBuffer.allocateDirect(1 << segmentBits));
    }

    void flush() {
      try {
        log("  flushing " + id);
        ByteBuffer buffer = result.get();
        writeChannel.write(buffer, offset);
        buffer.clear();
        log("  flushed " + id);
        synchronized (usedSegments) {
          usedSegments.set(id);
        }
        result = null;
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException(e);
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      } catch (ExecutionException e) {
        throw new RuntimeException(e);
      }
    }
  }

  record SegmentActions(
    List<Segment> flush, List<Segment> allocate, Segment result
  ) {}

  private void log(Object arg) {
//    LOGGER.warn(Thread.currentThread().getName() + " " + arg);
  }

  private synchronized SegmentActions getSegmentActions(AtomicInteger currentSeg, int value) {
    var before = segments.toString();
    currentSeg.set(value);
    var after = segments.toString();
    log("  before=" + before);
    log("  after=" + after);
    List<Segment> flush = new ArrayList<>();
    List<Segment> allocate = new ArrayList<>();
    var min = segments.stream().mapToInt(AtomicInteger::get).min().orElseThrow();
    if (min == Integer.MAX_VALUE) {
      // all done
      flush.addAll(writeBuffers.values());
      writeBuffers.clear();
      return new SegmentActions(flush, allocate, null);
    } else if (value == Integer.MAX_VALUE) {
      // this worker is done
      for (Integer key : writeBuffers.keySet()) {
        if (key < min) {
          var segment = writeBuffers.remove(key);
          if (segment != null) {
            flush.add(segment);
          }
        }
      }
      tail = min;
      slidingWindow.advanceTail(tail);
      return new SegmentActions(flush, allocate, null);
    }
    log("  min=" + min);
    while (tail < min) {
      if (writeBuffers.containsKey(tail)) {
        var segment = writeBuffers.remove(tail);
        if (segment != null) {
          flush.add(segment);
        }
      }
      log("  tail2=" + tail);
      tail++;
    }
    log("  advanceTail=" + tail);
    slidingWindow.advanceTail(tail);
    Segment result = writeBuffers.computeIfAbsent(value, id -> {
      var seg = new Segment(id);
      allocate.add(seg);
      return seg;
    });
    return new SegmentActions(flush, allocate, result);
  }

  @Override
  public Writer newWriter() {
    AtomicInteger currentSeg = new AtomicInteger(0);
    segments.add(currentSeg);
    return new Writer() {

      long lastSegment = -1;
      long segmentOffset = -1;
      ByteBuffer buffer = null;

      @Override
      public void close() {
        log("closing");
        var actions = getSegmentActions(currentSeg, Integer.MAX_VALUE);
        log("  close actions=" + actions);
        actions.flush.forEach(Segment::flush);
        log("  closed");
      }

      @Override
      public void put(long key, long value) {
        log("put(" + key + ", " + value + ")");
        long offset = key << 3;
        long segment = offset >>> segmentBits;
        if (segment > lastSegment) {
          if (segment >= Integer.MAX_VALUE) {
            throw new IllegalArgumentException("Segment " + segment + " > Integer.MAX_VALUE");
          }
          int segInt = (int) segment;
          // iterate through the tail-end and free up chunks that aren't needed anymore
          SegmentActions actions = getSegmentActions(currentSeg, segInt);
          // if this thread is allocating a new segment, then wait on allocating it
          // if this thread is just using one, then wait for it to become available
          log("  segments=" + segments + " actions=" + actions);

          actions.flush.forEach(Segment::flush);
          actions.allocate.forEach(Segment::allocate);

          // wait on adding a new buffer to head until the number of pending buffers is small enough
          buffer = actions.result.await();
          lastSegment = segment;
          segmentOffset = segment << segmentBits;
        }
        buffer.putLong((int) (offset - segmentOffset), value);
      }
    };
  }

  private volatile boolean inited = false;

  private void initOnce() {
    if (!inited) {
      synchronized (this) {
        if (!inited) {
          init();
          inited = true;
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
    if (readChannel != null) {
      readChannel.close();
      readChannel = null;
      FileUtils.delete(path);
    }
  }
}
