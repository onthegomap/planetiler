package com.onthegomap.planetiler.collection;

import com.onthegomap.planetiler.util.MemoryEstimator;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

class ArrayLongLongMapRam implements LongLongMap.ParallelWrites {

  private final int segmentBits;
  private final long segmentMask;
  private final int segmentSize;
  private final List<long[]> segments = new ArrayList<>();
  private final AtomicInteger numSegments = new AtomicInteger(0);

  ArrayLongLongMapRam() {
    this(17); // 1MB
  }

  ArrayLongLongMapRam(int segmentBits) {
    this.segmentBits = segmentBits;
    segmentMask = (1L << segmentBits) - 1;
    segmentSize = 1 << segmentBits;
  }

  private synchronized long[] getSegment(int index) {
    while (segments.size() <= index) {
      segments.add(null);
    }
    if (segments.get(index) == null) {
      numSegments.incrementAndGet();
      segments.set(index, new long[segmentSize]);
    }
    return segments.get(index);
  }

  @Override
  public Writer newWriter() {
    return new Writer() {

      long lastSegment = -1;
      long segmentOffset = -1;
      long[] buffer = null;

      @Override
      public void put(long key, long value) {
        long segment = key >>> segmentBits;
        if (segment > lastSegment) {
          if (segment >= Integer.MAX_VALUE) {
            throw new IllegalArgumentException("Segment " + segment + " > Integer.MAX_VALUE");
          }

          lastSegment = segment;
          segmentOffset = segment << segmentBits;
          buffer = getSegment((int) segment);
        }

        buffer[(int) (key - segmentOffset)] = value;
      }
    };
  }

  @Override
  public long get(long key) {
    int idx = (int) (key >>> segmentBits);
    int offset = (int) (key & segmentMask);
    if (idx >= segments.size()) {
      return LongLongMap.MISSING_VALUE;
    }
    long[] longs = segments.get(idx);
    if (longs == null) {
      return LongLongMap.MISSING_VALUE;
    }
    long result = longs[offset];
    return result == 0 ? LongLongMap.MISSING_VALUE : result;
  }

  @Override
  public long diskUsageBytes() {
    return 0;
  }

  @Override
  public long estimateMemoryUsageBytes() {
    return MemoryEstimator.estimateObjectArraySize(segments.size()) +
      MemoryEstimator.estimateLongArraySize(segmentSize) * numSegments.get();
  }

  @Override
  public void close() throws IOException {}
}
