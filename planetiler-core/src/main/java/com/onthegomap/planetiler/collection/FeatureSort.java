package com.onthegomap.planetiler.collection;

import com.onthegomap.planetiler.stats.Stats;
import com.onthegomap.planetiler.util.CloseableConsumer;
import com.onthegomap.planetiler.util.DiskBacked;
import com.onthegomap.planetiler.util.MemoryEstimator;
import com.onthegomap.planetiler.worker.WeightedHandoffQueue;
import com.onthegomap.planetiler.worker.Worker;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.stream.IntStream;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * A utility that accepts {@link SortableFeature} instances in any order and lets you iterate through them ordered by
 * {@link SortableFeature#key()}.
 * <p>
 * Only supports single-threaded writes and reads.
 */
@NotThreadSafe
interface FeatureSort extends Iterable<SortableFeature>, DiskBacked, MemoryEstimator.HasEstimate {
  /*
   * Idea graveyard (all too slow):
   * - sqlite (close runner-up to external merge sort - only 50% slower)
   * - mapdb b-tree
   * - rockdb
   * - berkeley db
   */

  /** Returns a feature sorter that sorts all features in memory. Suitable for toy examples (unit tests). */
  static FeatureSort newInMemory() {
    List<SortableFeature> list = Collections.synchronizedList(new ArrayList<>());
    return new FeatureSort() {
      @Override
      public void sort() {
        list.sort(Comparator.naturalOrder());
      }

      @Override
      public long numFeaturesWritten() {
        return list.size();
      }

      @Override
      public CloseableConsumer<SortableFeature> writerForThread() {
        return list::add;
      }

      @Override
      public long estimateMemoryUsageBytes() {
        return 0;
      }

      @Override
      public long diskUsageBytes() {
        return 0;
      }

      @Override
      public Iterator<SortableFeature> iterator() {
        return list.iterator();
      }

      @Override
      public Iterator<SortableFeature> iterator(int shard, int shards) {
        if (shard < 0 || shard >= shards) {
          throw new IllegalArgumentException("Bad shard params: shard=%d shards=%d".formatted(shard, shards));
        }
        return IntStream.range(0, list.size())
          .filter(d -> d % shards == shard)
          .mapToObj(list::get)
          .iterator();
      }

      @Override
      public int chunksToRead() {
        return list.size();
      }
    };
  }

  void sort();

  long numFeaturesWritten();

  /** Returns all elements in a list. WARNING: this will materialize all elements in-memory. */
  default List<SortableFeature> toList() {
    List<SortableFeature> list = new ArrayList<>();
    for (SortableFeature entry : this) {
      list.add(entry);
    }
    return list;
  }

  /**
   * Returns a new writer that can be used to write features from a single thread independent of writers used from other
   * threads.
   */
  CloseableConsumer<SortableFeature> writerForThread();

  @Override
  default Iterator<SortableFeature> iterator() {
    return iterator(0, 1);
  }

  /**
   * Returns an iterator over a subset of the features.
   *
   * @param shard  The index of this iterator
   * @param shards The total number of iterators that will be used
   * @return An iterator over a subset of features that when combined with all other iterators will iterate over the
   *         full set.
   */
  Iterator<SortableFeature> iterator(int shard, int shards);

  /**
   * Reads temp features using {@code threads} parallel threads and merges into a sorted list.
   *
   * @param stats   Stat tracker
   * @param threads The number of parallel read threads to spawn
   * @return a {@link ParallelIterator} with a handle to the new read threads that were spawned, and in {@link Iterable}
   *         that can be used to iterate over the results.
   */
  default ParallelIterator parallelIterator(Stats stats, int threads) {
    List<WeightedHandoffQueue<SortableFeature>> queues = IntStream.range(0, threads)
      .mapToObj(i -> new WeightedHandoffQueue<SortableFeature>(500, 10_000))
      .toList();
    Worker reader = new Worker("read", stats, threads, shard -> {
      try (var next = queues.get(shard)) {
        Iterator<SortableFeature> entries = iterator(shard, threads);
        while (entries.hasNext()) {
          next.accept(entries.next(), 1);
        }
      }
    });
    return new ParallelIterator(reader, LongMerger.mergeSuppliers(queues, SortableFeature.COMPARE_BYTES));
  }

  int chunksToRead();

  record ParallelIterator(Worker reader, @Override Iterator<SortableFeature> iterator)
    implements Iterable<SortableFeature> {}
}
