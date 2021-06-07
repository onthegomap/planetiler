package com.onthegomap.flatmap.collections;

import com.google.common.collect.Range;
import com.google.common.collect.TreeRangeSet;
import java.util.Iterator;
import java.util.PrimitiveIterator;

public class IntRange implements Iterable<Integer> {

  private final TreeRangeSet<Integer> rangeSet = TreeRangeSet.create();

  public IntRange addAll(IntRange range) {
    rangeSet.addAll(range.rangeSet);
    return this;
  }

  public IntRange removeAll(IntRange range) {
    rangeSet.removeAll(range.rangeSet);
    return this;
  }

  @Override
  public PrimitiveIterator.OfInt iterator() {
    return new Iter(rangeSet.asRanges().iterator());
  }

  public IntRange add(int a, int b) {
    rangeSet.add(Range.closedOpen(a, b + 1));
    return this;
  }

  public IntRange remove(int a) {
    rangeSet.remove(Range.closedOpen(a, a + 1));
    return this;
  }

  public boolean contains(int y) {
    return rangeSet.contains(y);
  }

  public IntRange intersect(IntRange other) {
    rangeSet.removeAll(other.rangeSet.complement());
    return this;
  }

  private static class Iter implements PrimitiveIterator.OfInt {

    private final Iterator<Range<Integer>> rangeIter;
    Range<Integer> range;
    Integer cur;
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
