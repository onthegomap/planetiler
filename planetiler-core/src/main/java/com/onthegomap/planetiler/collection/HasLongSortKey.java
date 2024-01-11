package com.onthegomap.planetiler.collection;

/**
 * An item with a {@code long key} that can be used for sorting/grouping.
 * <p>
 * These items can be sorted or grouped by {@link FeatureSort}/{@link FeatureGroup} implementations. Sorted lists can
 * also be merged using {@link LongMerger}.
 */
public interface HasLongSortKey {
  /** Value to sort/group items by. */
  long key();
}
