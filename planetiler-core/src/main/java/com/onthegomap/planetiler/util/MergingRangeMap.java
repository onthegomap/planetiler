package com.onthegomap.planetiler.util;

import com.google.common.collect.Range;
import com.google.common.collect.RangeMap;
import com.google.common.collect.TreeRangeMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.BinaryOperator;
import java.util.function.UnaryOperator;

/**
 * A mapping from disjoint ranges to values that merges values assigned to overlapping ranges.
 * <p>
 * This is similar {@link RangeMap} that merges overlapping values and coalesces ranges with identical values by
 * default.
 * <p>
 * For example:
 * {@snippet :
 * MergingRangeMap map = MergingRangeMap.unitMap();
 * map.put(0, 0.5, Map.of("key", "value"));
 * // overrides value for key from [0.4, 0.5), and sets from [0.5, 1)
 * map.put(0.4, 1, Map.of("key", "value2"));
 * // adds key2 from [0.9, 1)
 * map.put(0.9, 1, Map.of("key2", "value3"));
 * // returns [
 * //   Partial[start=0.0, end=0.4, value={key=value}],
 * //   Partial[start=0.4, end=0.9, value={key=value2}],
 * //   Partial[start=0.9, end=1.0, value={key2=value3, key=value2}]
 * // ]
 * map.result();
 * }
 */
public class MergingRangeMap<T> {

  private final RangeMap<Double, T> items = TreeRangeMap.create();
  private final BinaryOperator<T> merger;

  private MergingRangeMap(double lo, double hi, T identity, BinaryOperator<T> merger) {
    items.put(Range.closedOpen(lo, hi), identity);
    this.merger = merger;
  }

  /**
   * Returns a new range map where values are {@link Map Maps} that get merged together when they overlap from
   * {@code [0, 1)}.
   */
  public static <K, V> MergingRangeMap<Map<K, V>> unitMap() {
    return unit(Map.of(), MapUtil::merge);
  }

  /**
   * Returns a new range map with {@code identity} from {@code [0, 1)} and {@code merger} as the default merging
   * function for {@link #put(Range, Object)}.
   */
  public static <T> MergingRangeMap<T> unit(T identity, BinaryOperator<T> merger) {
    return create(0, 1, identity, merger);
  }

  /**
   * Returns a new range map with {@code identity} from {@code [lo, hi)} and {@code merger} as the default merging
   * function for {@link #put(Range, Object)}.
   */
  public static <T> MergingRangeMap<T> create(double lo, double hi, T identity, BinaryOperator<T> merger) {
    return new MergingRangeMap<>(lo, hi, identity, merger);
  }

  /** Returns the distinct set of ranges and their values where adjacent maps with identical values are merged. */
  public List<Partial<T>> result() {
    List<Partial<T>> result = new ArrayList<>();
    for (var entry : items.asMapOfRanges().entrySet()) {
      result.add(new Partial<>(entry.getKey().lowerEndpoint(), entry.getKey().upperEndpoint(), entry.getValue()));
    }
    return result;
  }

  /**
   * Change each of the distinct values over {@code range} to the result of applying {@code operator} to the existing
   * value.
   */
  public void update(Range<Double> range, UnaryOperator<T> operator) {
    var overlaps = new ArrayList<>(items.subRangeMap(range).asMapOfRanges().entrySet());
    for (var overlap : overlaps) {
      items.putCoalescing(overlap.getKey(), operator.apply(overlap.getValue()));
    }
  }

  /**
   * Merge {@code next} into the value associated with all ranges that overlap {@code [start, end)} using the default
   * merging function.
   */
  public void put(double start, double end, T next) {
    put(Range.closedOpen(start, end), next);
  }

  /**
   * Merge {@code next} into the value associated with all ranges that overlap {@code range} using the default merging
   * function.
   */
  public void put(Range<Double> range, T next) {
    update(range, prev -> merger.apply(prev, next));
  }

  /** Subset of the range and value that applies to it. */
  public record Partial<T>(double start, double end, T value) {}
}
