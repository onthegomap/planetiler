package com.onthegomap.planetiler.collection;

import com.google.common.collect.Range;
import com.google.common.collect.TreeRangeSet;
import java.util.Iterator;
import java.util.PrimitiveIterator;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * A set of ints backed by a {@link TreeRangeSet} to efficiently represent large continuous ranges.
 * <p>
 * This makes iterating through tile coordinates inside ocean polygons significantly faster.
 */
@SuppressWarnings("UnstableApiUsage")
@NotThreadSafe
public class IntRangeSet implements Iterable<Integer> {

  private final TreeRangeSet<Integer> rangeSet = TreeRangeSet.create();

  /** Mutates and returns this range set, adding all elements in {@code other} to it. */
  public IntRangeSet addAll(IntRangeSet other) {
    rangeSet.addAll(other.rangeSet);
    return this;
  }

  /** Mutates and returns this range set, removing all elements in {@code other} from it. */
  public IntRangeSet removeAll(IntRangeSet other) {
    rangeSet.removeAll(other.rangeSet);
    return this;
  }

  @Override
  public PrimitiveIterator.OfInt iterator() {
    return new Iter(rangeSet.asRanges().iterator());
  }

  /** Mutates and returns this range set, with range {@code a} to {@code b} (inclusive) added. */
  public IntRangeSet add(int a, int b) {
    rangeSet.add(Range.closedOpen(a, b + 1));
    return this;
  }

  /** Mutates and returns this range set, with {@code a} removed. */
  public IntRangeSet remove(int a) {
    rangeSet.remove(Range.closedOpen(a, a + 1));
    return this;
  }

  public boolean contains(int y) {
    return rangeSet.contains(y);
  }

  /** Mutates and returns this range set to remove all elements not in {@code other} */
  public IntRangeSet intersect(IntRangeSet other) {
    rangeSet.removeAll(other.rangeSet.complement());
    return this;
  }

  /** Iterate through all ints in this range */
  private static class Iter implements PrimitiveIterator.OfInt {

    private final Iterator<Range<Integer>> rangeIter;
    Range<Integer> range;
    int cur;
    boolean hasNext = true;

    private Iter(Iterator<Range<Integer>> rangeIter) {
      this.rangeIter = rangeIter;
      advance();
    }

    private void advance() {
      while (true) {
        if (range != null && cur < range.upperEndpoint() - 1) {
          cur++;
          return;
        } else if (rangeIter.hasNext()) {
          range = rangeIter.next();
          cur = range.lowerEndpoint() - 1;
        } else {
          hasNext = false;
          return;
        }
      }
    }

    @Override
    public int nextInt() {
      int result = cur;
      advance();
      return result;
    }

    @Override
    public boolean hasNext() {
      return hasNext;
    }
  }
}
