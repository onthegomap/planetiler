package com.onthegomap.planetiler.collection;

import com.onthegomap.planetiler.util.MemoryEstimator;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class ArrayLongLongMapRam implements LongLongMap.ParallelWrites {

  static final int segmentBits = 20; // 8MB
  static final long segmentMask = (1L << segmentBits) - 1;
  static final int segmentSize = 1 << segmentBits;
  private static final List<long[]> segments = new ArrayList<>();
  private final AtomicInteger numSegments = new AtomicInteger(0);

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
    long result = segments.get(idx)[offset];
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
