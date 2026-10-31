package com.onthegomap.planetiler.collection;

import com.carrotsearch.hppc.IntArrayList;
import com.onthegomap.planetiler.util.MemoryEstimator;
import java.util.Arrays;

/**
 * A map from non-negative {@code long} keys to non-negative {@code long} values where keys and values are both added in
 * ascending order.
 * <p>
 * Keys are stored in 512-entry pages where a page only exists if it has values. Within each page, sparse values are
 * packed into a dense array using a presence bitmap to indicate which keys have a value.
 */
class SparseMonotonicLongMap implements MemoryEstimator.HasEstimate {

  // 512 entries per page
  private static final int PAGE_BITS = 9;
  private static final int PAGE_SIZE = 1 << PAGE_BITS;
  private static final int PAGE_MASK = PAGE_SIZE - 1;

  private Page[] pages = new Page[16];
  private int numPages = 0;
  private Page lastPage = null;
  private long lastKey = -1;

  /** Adds {@code value} for {@code key}, which must be greater than any key added before. */
  void add(long key, long value) {
    if (key <= lastKey) {
      throw new IllegalArgumentException("Keys must be added in ascending order, got " + key + " after " +
        lastKey);
    }
    lastKey = key;
    int pageNum = Math.toIntExact(key >>> PAGE_BITS);
    if (pageNum >= numPages) {
      if (lastPage != null) {
        // the previous page ends where this one starts so callers can get value or next value for a key
        lastPage.append(value);
        lastPage.trim();
      }
      if (pageNum >= pages.length) {
        pages = Arrays.copyOf(pages, Math.max(pageNum + 1, pages.length * 2));
      }
      pages[pageNum] = lastPage = new Page(value);
      numPages = pageNum + 1;
    }
    lastPage.add(key, value);
  }

  /** Returns the value of {@code key}, or -1 if it was not added. */
  long get(long key) {
    Page page = page(key);
    return page == null ? -1 : page.get(key);
  }

  /**
   * Returns the value of the next key added after {@code key}, which must have been added, or -1 if it is the last one.
   */
  long next(long key) {
    Page page = page(key);
    if (page == null) {
      throw new IllegalArgumentException("Key " + key + " was not added");
    }
    return page.next(key);
  }

  private Page page(long key) {
    long pageNum = key >>> PAGE_BITS;
    return key < 0 || pageNum >= numPages ? null : pages[(int) pageNum];
  }

  @Override
  public long estimateMemoryUsageBytes() {
    long result = MemoryEstimator.estimateSize(pages);
    for (int i = 0; i < numPages; i++) {
      Page page = pages[i];
      if (page != null) {
        result += MemoryEstimator.CLASS_HEADER_BYTES + MemoryEstimator.LONG_BYTES + 2 * MemoryEstimator.POINTER_BYTES +
          MemoryEstimator.estimateSize(page.bitmap) + MemoryEstimator.estimateSize(page.values);
      }
    }
    return result;
  }

  /**
   * Stores the value of each key in the page that was added, using a bitmap of which keys were added to find its index,
   * and storing values as ints relative to the first one to use half the memory.
   */
  private static final class Page {

    // one bit per key in the page, set if the key was added
    private final long[] bitmap = new long[PAGE_SIZE / Long.SIZE];
    // all values stored as a delta from base
    private final long base;
    // the value of each added key, in order; plus the next page's first value once it starts, so the last key here
    // knows where it ends
    private final IntArrayList values = new IntArrayList();

    private Page(long base) {
      this.base = base;
    }

    private void add(long key, long value) {
      int bit = (int) (key & PAGE_MASK);
      int word = bit >>> 6; // 64 bits per long in bitmap
      bitmap[word] |= 1L << bit;
      append(value);
    }

    private void append(long value) {
      values.add(Math.toIntExact(value - base));
    }

    private void trim() {
      values.trimToSize();
    }

    /** Returns the value of {@code key}, or -1 if it was not added. */
    private long get(long key) {
      int index = indexOf(key);
      return index < 0 ? -1 : base + values.get(index);
    }

    /** Returns the value of the key added after {@code key}, or -1 if there is none. */
    private long next(long key) {
      int index = indexOf(key);
      if (index < 0) {
        throw new IllegalArgumentException("Key " + key + " was not added");
      }
      return index + 1 < values.size() ? base + values.get(index + 1) : -1;
    }

    /** Returns the index of {@code key} in {@link #values}, or -1 if it was not added. */
    private int indexOf(long key) {
      int bit = (int) (key & PAGE_MASK);
      int word = bit >>> 6; // 64 bits per long in bitmap
      long bits = bitmap[word];
      if ((bits & (1L << bit)) == 0) {
        return -1;
      }
      int index = Long.bitCount(bits & ((1L << bit) - 1));
      for (int i = 0; i < word; i++) {
        index += Long.bitCount(bitmap[i]);
      }
      return index;
    }
  }
}
