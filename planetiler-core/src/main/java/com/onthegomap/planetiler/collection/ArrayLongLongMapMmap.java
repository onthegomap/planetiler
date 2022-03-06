package com.onthegomap.planetiler.collection;

import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;

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
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

public class ArrayLongLongMapMmap implements LongLongMap.ParallelWrites {

  FileChannel writeChannel;
  static final int segmentBits = 27; // 128MB
  static final int maxPendingSegments = 20; // 1GB
  static final long segmentMask = (1L << segmentBits) - 1;
  static final long segmentBytes = 1 << segmentBits;
  private final SlidingWindow slidingWindow = new SlidingWindow(maxPendingSegments);
  final Path path;
  MappedByteBuffer[] segmentsArray;
  CopyOnWriteArrayList<AtomicInteger> segments = new CopyOnWriteArrayList<>();
  final ConcurrentMap<Integer, Segment> writeBuffers = new ConcurrentHashMap<>();
  private FileChannel readChannel = null;
  private volatile int tail = 0;

  public ArrayLongLongMapMmap(Path path) {
    this.path = path;
    try {
      writeChannel = FileChannel.open(path, WRITE, CREATE);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  public void init() {
    try {
      System.err.println("init: " + writeBuffers);
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
        MappedByteBuffer buffer = readChannel.map(FileChannel.MapMode.READ_ONLY, segmentStart, segmentLength);
        // TODO madvise
        segmentsArray[i++] = buffer;
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
      } catch (InterruptedException | ExecutionException e) {
        throw new RuntimeException(e);
      }
    }

    void allocate() {
      slidingWindow.waitUntilInsideWindow(id);
      System.err.println("    " + Thread.currentThread().getName() + " allocating " + id);
      result.complete(ByteBuffer.allocateDirect(1 << segmentBits));
    }

    synchronized void flush() {
      try {
        System.err.println("    " + Thread.currentThread().getName() + " flushing " + id);
        ByteBuffer buffer = result.get();
        writeChannel.write(buffer, offset);
        buffer.clear();
        result = null;
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      } catch (ExecutionException | InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
  }

  record SegmentActions(
    List<Segment> flush, List<Segment> allocate, Segment result
  ) {}

  private synchronized SegmentActions getSegmentActions(AtomicInteger currentSeg, int value) {
    var before = segments.toString();
    currentSeg.set(value);
    var after = segments.toString();
    System.err.println(Thread.currentThread().getName() + "\n    before=" + before + "\n    after= " + after);
    List<Segment> flush = new ArrayList<>();
    List<Segment> allocate = new ArrayList<>();
    var min = segments.stream().mapToInt(AtomicInteger::get).min().orElseThrow();
    if (min == Integer.MAX_VALUE || value == Integer.MAX_VALUE) {
      for (Integer key : writeBuffers.keySet()) {
        var segment = writeBuffers.remove(key);
        if (segment != null) {
          flush.add(segment);
        }
      }
      return new SegmentActions(flush, allocate, null);
    }
    while (tail < min) {
      var segment = writeBuffers.remove(tail);
      if (segment != null) {
        flush.add(segment);
      }
      tail++;
    }
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
        System.err.println(Thread.currentThread().getName() + " closing");
        getSegmentActions(currentSeg, Integer.MAX_VALUE).flush.forEach(Segment::flush);
      }

      @Override
      public void put(long key, long value) {
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
          System.err.println(
            Thread.currentThread().getName() + " segments=" + segments + " actions=" + actions);

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
    int offset = (int) (byteOffset & segmentMask);
    long result = segmentsArray[idx].getLong(offset);
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
