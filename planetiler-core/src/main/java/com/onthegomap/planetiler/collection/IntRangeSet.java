package com.onthegomap.planetiler.collection;

import java.util.PrimitiveIterator;
import javax.annotation.concurrent.NotThreadSafe;
import org.roaringbitmap.PeekableIntIterator;
import org.roaringbitmap.RoaringBitmap;

/**
 * A set of ints backed by a {@link RoaringBitmap} to efficiently represent large continuous ranges.
 * <p>
 * This makes iterating through tile coordinates inside ocean polygons significantly faster.
 */
@NotThreadSafe
public class IntRangeSet implements Iterable<Integer> {

  private final RoaringBitmap bitmap = new RoaringBitmap();

  /** Mutates and returns this range set, adding all elements in {@code other} to it. */
  public IntRangeSet addAll(IntRangeSet other) {
    bitmap.or(other.bitmap);
    return this;
  }

  /** Mutates and returns this range set, removing all elements in {@code other} from it. */
  public IntRangeSet removeAll(IntRangeSet other) {
    bitmap.andNot(other.bitmap);
    return this;
  }

  @Override
  public PrimitiveIterator.OfInt iterator() {
    return new Iter(bitmap.getIntIterator());
  }

  /** Mutates and returns this range set, with range {@code a} to {@code b} (inclusive) added. */
  public IntRangeSet add(int a, int b) {
    bitmap.add(a, (long) b + 1);
    return this;
  }

  /** Mutates and returns this range set, with {@code a} removed. */
  public IntRangeSet remove(int a) {
    bitmap.remove(a);
    return this;
  }

  public boolean contains(int y) {
    return bitmap.contains(y);
  }

  /** Returns the underlying {@link RoaringBitmap} for this int range. */
  public RoaringBitmap bitmap() {
    return bitmap;
  }

  /** Mutates and returns this range set to remove all elements not in {@code other} */
  public IntRangeSet intersect(IntRangeSet other) {
    bitmap.and(other.bitmap);
    return this;
  }

  /** Iterate through all ints in this range */
  private record Iter(PeekableIntIterator iter) implements PrimitiveIterator.OfInt {

    @Override
    public boolean hasNext() {
      return iter.hasNext();
    }

    @Override
    public int nextInt() {
      return iter.next();
    }
  }
}
