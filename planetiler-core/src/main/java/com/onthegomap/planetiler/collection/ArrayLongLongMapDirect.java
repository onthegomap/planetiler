package com.onthegomap.planetiler.collection;

import com.onthegomap.planetiler.util.MemoryEstimator;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class ArrayLongLongMapDirect implements LongLongMap.ParallelWrites {

  private final int segmentBits;
  private final long segmentMask;
  private final int segmentSize;
  private final List<ByteBuffer> segments = new ArrayList<>();
  private final AtomicInteger numSegments = new AtomicInteger(0);

  public ArrayLongLongMapDirect() {
    this(23); // 8MB
  }

  public ArrayLongLongMapDirect(int segmentBits) {
    this.segmentBits = segmentBits;
    segmentMask = (1L << segmentBits) - 1;
    segmentSize = 1 << segmentBits;
  }

  private synchronized ByteBuffer getSegment(int index) {
    while (segments.size() <= index) {
      segments.add(null);
    }
    if (segments.get(index) == null) {
      numSegments.incrementAndGet();
      segments.set(index, ByteBuffer.allocateDirect(segmentSize));
    }
    return segments.get(index);
  }

  @Override
  public Writer newWriter() {
    return new Writer() {

      long lastSegment = -1;
      long segmentOffset = -1;
      ByteBuffer buffer = null;

      @Override
      public void put(long key, long value) {
        long offset = key << 3;
        long segment = offset >>> segmentBits;
        if (segment > lastSegment) {
          if (segment >= Integer.MAX_VALUE) {
            throw new IllegalArgumentException("Segment " + segment + " > Integer.MAX_VALUE");
          }

          lastSegment = segment;
          segmentOffset = segment << segmentBits;
          buffer = getSegment((int) segment);
        }

        buffer.putLong((int) (offset - segmentOffset), value);
      }
    };
  }

  @Override
  public long get(long key) {
    long byteOffset = key << 3;
    int idx = (int) (byteOffset >>> segmentBits);
    if (idx >= segments.size()) {
      return LongLongMap.MISSING_VALUE;
    }
    int offset = (int) (byteOffset & segmentMask);
    ByteBuffer byteBuffer = segments.get(idx);
    if (byteBuffer == null) {
      return LongLongMap.MISSING_VALUE;
    }
    long result = byteBuffer.getLong(offset);
    return result == 0 ? LongLongMap.MISSING_VALUE : result;
  }

  @Override
  public long diskUsageBytes() {
    return 0;
  }

  @Override
  public long estimateMemoryUsageBytes() {
    return MemoryEstimator.estimateObjectArraySize(segments.size())
      + MemoryEstimator.estimateLongArraySize(segmentSize) * numSegments.get();
  }

  @Override
  public void close() throws IOException {
  }
}
