package com.onthegomap.planetiler.collection;

import com.onthegomap.planetiler.util.DiskBacked;
import com.onthegomap.planetiler.util.MemoryEstimator;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * A utility that accepts {@link SortableFeature} instances in any order and lets you iterate
 * through them ordered by {@link SortableFeature#key()}.
 *
 * <p>Only supports single-threaded writes and reads.
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

  /**
   * Returns a feature sorter that sorts all features in memory. Suitable for toy examples (unit
   * tests).
   */
  static FeatureSort newInMemory() {
    List<SortableFeature> list = new ArrayList<>();
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
      public void add(SortableFeature newEntry) {
        list.add(newEntry);
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

  void add(SortableFeature newEntry);
}
