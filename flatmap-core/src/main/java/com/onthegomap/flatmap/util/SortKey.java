package com.onthegomap.flatmap.util;

import com.onthegomap.flatmap.FeatureCollector;
import com.onthegomap.flatmap.collection.FeatureGroup;

/**
 * A utility to compute integer sort keys for {@link FeatureCollector.Feature#setSortKey(int)} where the integer key
 * ordering approximates the desired ordering.
 * <p>
 * Sort keys get packed into {@link FeatureGroup#SORT_KEY_BITS} bits, so sort key components need to specify the range
 * and number of levels the range gets packed into.  Requests that exceed the total number of available levels will
 * fail.
 * <p>
 * To sort by a field descending, specify its range from high to low.
 * <p>
 * For example this SQL ordering:
 * <pre>{@code
 * ORDER BY rank ASC,
 * population DESC,
 * length(name) ASC
 * }</pre>
 * <p>
 * would become:
 * <pre>{@code
 * feature.setSortKey(
 *   SortKey
 *     .orderByInt(rank, MIN_RANK, MAX_RANK)
 *     .thenByLog(population, MAX_POPULATION, MIN_POPULATION, NUM_LEVELS)
 *     .thenByInt(name.length(), 0, MAX_LENGTH)
 *     .get()
 * )
 * }</pre>
 */
public class SortKey {

  private static final int MAX = FeatureGroup.SORT_KEY_MAX - FeatureGroup.SORT_KEY_MIN;
  private long possibleValues = 1;
  private int result = 0;

  private SortKey() {
  }

  /** Returns a new sort key where elements with {@code value == true} sort after ones where {@code value == false} */
  public static SortKey orderByTruesLast(boolean value) {
    return new SortKey().thenByTruesLast(value);
  }

  /** Returns a new sort key where elements with {@code value == true} sort after ones where {@code value == false} */
  public static SortKey orderByTruesFirst(boolean value) {
    return new SortKey().thenByTruesFirst(value);
  }

  /**
   * Returns a new sort key where elements with {@code value == start} appear before elements with {@code value ==
   * end}.
   */
  public static SortKey orderByInt(int value, int start, int end) {
    return new SortKey().thenByInt(value, start, end);
  }

  /**
   * Returns a new sort key where elements with {@code value == start} appear before elements with {@code value == end}
   * and the entire range is compressed into {@code levels} distinct levels for comparison.
   */
  public static SortKey orderByDouble(double value, double start, double end, int levels) {
    return new SortKey().thenByDouble(value, start, end, levels);
  }

  /**
   * Returns a new sort key where elements with {@code value == start} appear before elements with {@code value == end}
   * and the log of the entire range is compressed into the maximum allowed number of distinct levels for comparison.
   */
  public static SortKey orderByLog(double value, double start, double end) {
    return new SortKey().thenByLog(value, start, end, MAX);
  }

  /**
   * Returns a new sort key where elements with {@code value == start} appear before elements with {@code value == end}
   * and the log of the entire range is compressed into {@code levels} distinct levels for comparison.
   */
  public static SortKey orderByLog(double value, double min, double max, int levels) {
    return new SortKey().thenByLog(value, min, max, levels);
  }

  /**
   * Breaks the input range {@code [min, max]} into {@code levels} discrete levels, and returns an integer corresponding
   * to the level for {@code value}.
   */
  private static int doubleRangeToInt(double value, double min, double max, int levels) {
    return Math.min(levels - 1, (int) Math.floor(((value - min) / (max - min)) * levels));
  }

  /** Returns an integer sort key reflecting the ordering of sort fields added so far. */
  public int get() {
    return result + FeatureGroup.SORT_KEY_MIN;
  }

  /**
   * Adds a field to this sort key where if all previous values are equal, then elements where {@code value == true}
   * sort before elements where {@code value == false}.
   */
  public SortKey thenByTruesFirst(boolean value) {
    return thenByInt(value ? 0 : 1, 0, 1);
  }

  /**
   * Adds a field to this sort key where if all previous values are equal, then elements where {@code value == true}
   * sort after elements where {@code value == false}.
   */
  public SortKey thenByTruesLast(boolean value) {
    return thenByInt(value ? 1 : 0, 0, 1);
  }

  /**
   * Adds a field to this sort key where if all previous values are equal, then elements where {@code value == start}
   * sort before elements where {@code value == end}.
   */
  public SortKey thenByInt(int value, int start, int end) {
    if (start > end) {
      return thenByInt(start - value, end, start);
    }
    int levels = end + 1 - start;
    if (value < start || value > end) {
      value = Math.max(start, Math.min(end, value));
    }
    return accumulate(value, start, levels);
  }

  /**
   * Adds a field to this sort key where if all previous values are equal, then elements where {@code value == start}
   * sort before elements where {@code value == end} and the range {@code [min, max]} is broken into {@code levels}
   * discrete levels.
   */
  public SortKey thenByDouble(double value, double start, double end, int levels) {
    assert levels > 0 : "levels must be > 0 got " + levels;
    if (start > end) {
      return thenByDouble(start - value, end, start, levels);
    }
    if (value < start || value > end) {
      value = Math.max(start, Math.min(end, value));
    }

    int intVal = doubleRangeToInt(value, start, end, levels);
    return accumulate(intVal, 0, levels);
  }

  /**
   * Adds a field to this sort key where if all previous values are equal, then elements where {@code value == start}
   * sort before elements where {@code value == end} and the range {@code [min, max]} is broken into {@code levels}
   * discrete levels based on the log of the input.
   */
  public SortKey thenByLog(double value, double start, double end, int levels) {
    assert levels > 0 : "levels must be > 0 got " + levels;
    if (start > end) {
      return thenByLog(start / value, end, start, levels);
    }
    assert start > 0 : "log thresholds must be > 0 got [" + start + ", " + end + "]";
    if (value < start || value > end) {
      value = Math.max(start, Math.min(end, value));
    }

    int intVal = doubleRangeToInt(Math.log(value), Math.log(start), Math.log(end), levels);
    return accumulate(intVal, 0, levels);
  }

  /** Adds a new component {@code value} to the key. */
  private SortKey accumulate(int value, int min, int levels) {
    possibleValues *= levels;
    if (possibleValues > MAX) {
      throw new IllegalArgumentException("Too many possible values: " + possibleValues);
    }
    result = (result * levels) + value - min;
    return this;
  }
}
