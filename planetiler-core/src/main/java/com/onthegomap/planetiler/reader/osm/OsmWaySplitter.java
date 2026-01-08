package com.onthegomap.planetiler.reader.osm;

import com.carrotsearch.hppc.IntArrayList;
import com.carrotsearch.hppc.LongArrayList;
import com.onthegomap.planetiler.util.MemoryEstimator;
import java.io.Closeable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.stream.IntStream;
import org.roaringbitmap.RoaringBitmap;

/**
 * Utility that finds nodes where ways intersect.
 * <p>
 * We process the list of node IDs in each way once. If a node ID appears more than once (even in the same way) then it
 * is an intersection point.
 */
public interface OsmWaySplitter extends MemoryEstimator.HasEstimate {

  /** Returns a naive splitter backed by a synchronized in-memory {@link HashMap}. */
  static OsmWaySplitter mapSplitter() {
    return new MapSplitter();
  }

  /**
   * Returns a new optimized splitter backed by {@link RoaringBitmap RoaringBitmaps} where each thread updates local
   * bitmaps and periodically merges them into shared bitmaps to avoid contention.
   * <p>
   * With 1.1B ways and 10B nodes, this uses at most 3GB (1.5GB after {@link #finish()} is called).
   */
  static OsmWaySplitter roaringBitmapSplitter() {
    return new RoaringBitmapSplitter(100_000_000_000L, 23, 10_000);
  }

  /** Returns a new thread-local writer that should be used to add ways to this splitter. */
  PerThreadWriter writerForThread();

  /** Returns index of shared nodes that this way should get split, at excluding the first and last node. */
  default IntArrayList getSplitIndices(LongArrayList nodeIds) {
    IntArrayList indices = new IntArrayList();
    for (var node : nodeIds) {
      if (node.index > 0 && node.index < nodeIds.size() - 1 && isShared(node.value)) {
        indices.add(node.index);
      }
    }
    return indices;
  }

  /** Returns {@code true} if this node is used more than once in a way. */
  boolean isShared(long nodeId);

  /** Call this after adding all ways to clean up unused memory. */
  default void finish() {}

  @Override
  default long estimateMemoryUsageBytes() {
    return 0;
  }

  @FunctionalInterface
  interface PerThreadWriter extends Closeable {

    void addWay(LongArrayList nodeIds);

    @Override
    default void close() {}
  }


  class MapSplitter implements OsmWaySplitter {

    Map<Long, Integer> nodeCounts = new HashMap<>();

    @Override
    public PerThreadWriter writerForThread() {
      return nodes -> {
        synchronized (this) {
          for (var nodeId : nodes) {
            nodeCounts.merge(nodeId.value, 1, Integer::sum);
          }
        }
      };
    }

    @Override
    public boolean isShared(long nodeId) {
      return nodeCounts.get(nodeId) > 1;
    }
  }

  class RoaringBitmapSplitter implements OsmWaySplitter {
    // global bitmaps to track collisions across all threads
    private final RoaringBitmap[] allVisited;
    private final RoaringBitmap[] allShared;
    private final Object[] locks;

    private final int lowerBits;
    private final int lowerBitMask;
    private final int numBitmaps;
    private final int flushLimit;

    /**
     * @param maxId      Maximum node ID this supports
     * @param lowerBits  Number of bits to store in individual roaring bitmaps
     * @param flushLimit Number of writes to a partition before we flush it to shared bitmaps
     */
    RoaringBitmapSplitter(long maxId, int lowerBits, int flushLimit) {
      if (lowerBits > 30) {
        throw new IllegalArgumentException("Must be <=30 lower bits");
      }
      int numPerBitmap = 1 << lowerBits;
      this.flushLimit = flushLimit;
      numBitmaps = Math.toIntExact(maxId / numPerBitmap + 1L);
      locks = IntStream.range(0, numBitmaps).mapToObj(i -> new Object()).toArray(Object[]::new);
      allVisited = new RoaringBitmap[numBitmaps];
      allShared = new RoaringBitmap[numBitmaps];
      this.lowerBits = lowerBits;
      this.lowerBitMask = (1 << lowerBits) - 1;
    }


    @Override
    public PerThreadWriter writerForThread() {
      // thread-local bitmaps updated on every way
      RoaringBitmap[] visited = new RoaringBitmap[numBitmaps];
      RoaringBitmap[] shared = new RoaringBitmap[numBitmaps];
      int[] counts = new int[numBitmaps];

      return new PerThreadWriter() {

        @Override
        public void addWay(LongArrayList nodes) {
          for (int i = 0; i < nodes.size(); i++) {
            var node = nodes.get(i);
            int index = index(node);
            int offset = offset(node);
            if (visited[index] == null) {
              visited[index] = new RoaringBitmap();
              shared[index] = new RoaringBitmap();
            }
            if (!visited[index].checkedAdd(offset)) {
              shared[index].add(offset);
            }
            if (counts[index]++ > flushLimit) {
              flush(index);
            }
          }
        }

        void flush(int index) {
          if (counts[index] > 0) {
            synchronized (locks[index]) {
              if (allVisited[index] == null) {
                allVisited[index] = visited[index].clone();
                allShared[index] = shared[index].clone();
              } else if (visited[index] != null) {
                // Merge intermediate bitmaps into result
                RoaringBitmap crossPartitionCollisions = RoaringBitmap.and(allVisited[index], visited[index]);
                allShared[index].or(crossPartitionCollisions);
                allVisited[index].or(visited[index]);
                allShared[index].or(shared[index]);
              }
            }
            visited[index] = null;
            shared[index] = null;
            counts[index] = 0;
          }
        }

        @Override
        public void close() {
          for (int i = 0; i < counts.length; i++) {
            flush(i);
          }
        }
      };
    }

    @Override
    public void finish() {
      Arrays.fill(allVisited, null);
    }

    @Override
    public boolean isShared(long nodeId) {
      RoaringBitmap shared = allShared[index(nodeId)];
      return shared != null && shared.contains(offset(nodeId));
    }

    private int offset(long nodeId) {
      return (int) (nodeId & lowerBitMask);
    }

    private int index(long nodeId) {
      return (int) (nodeId >> lowerBits);
    }

    @Override
    public long estimateMemoryUsageBytes() {
      return MemoryEstimator.estimateSize(locks) + MemoryEstimator.estimateSize(allVisited) +
        MemoryEstimator.estimateSize(allShared) +
        Arrays.stream(allVisited).filter(Objects::nonNull).mapToLong(RoaringBitmap::getSizeInBytes).sum() +
        Arrays.stream(allShared).filter(Objects::nonNull).mapToLong(RoaringBitmap::getSizeInBytes).sum();
    }
  }
}
