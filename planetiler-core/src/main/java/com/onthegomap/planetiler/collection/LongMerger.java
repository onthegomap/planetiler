package com.onthegomap.planetiler.collection;

import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.function.Supplier;

/**
 * A utility for merging sorted lists of items with a {@code long} key to sort by.
 */
public class LongMerger {
  // Has a general-purpose KWayMerge implementation using a min heap and specialized (faster)
  // TwoWayMerge/ThreeWayMerge implementations when a small number of lists are being merged.

  private LongMerger() {}

  /** Merges sorted items from {@link Supplier Suppliers} that return {@code null} when there are no items left. */
  public static <T extends HasLongSortKey> Iterator<T> mergeSuppliers(List<? extends Supplier<T>> suppliers,
    Comparator<T> tieBreaker) {
    return mergeIterators(suppliers.stream().map(SupplierIterator::new).toList(), tieBreaker);
  }

  /** Merges sorted iterators into a combined iterator over all the items. */
  public static <T extends HasLongSortKey> Iterator<T> mergeIterators(List<? extends Iterator<T>> iterators,
    Comparator<T> tieBreaker) {
    return switch (iterators.size()) {
      case 0 -> Collections.emptyIterator();
      case 1 -> iterators.get(0);
      case 2 -> new TwoWayMerge<>(iterators.get(0), iterators.get(1), tieBreaker);
      case 3 -> new ThreeWayMerge<>(iterators.get(0), iterators.get(1), iterators.get(2), tieBreaker);
      default -> new KWayMerge<>(iterators, tieBreaker);
    };
  }

  private static class TwoWayMerge<T extends HasLongSortKey> implements Iterator<T> {

    private final Comparator<T> tieBreaker;
    T a, b;
    long ak = Long.MAX_VALUE, bk = Long.MAX_VALUE;
    final Iterator<T> inputA, inputB;

    TwoWayMerge(Iterator<T> inputA, Iterator<T> inputB, Comparator<T> tieBreaker) {
      this.inputA = inputA;
      this.inputB = inputB;
      this.tieBreaker = tieBreaker;
      if (inputA.hasNext()) {
        a = inputA.next();
        ak = a.key();
      }
      if (inputB.hasNext()) {
        b = inputB.next();
        bk = b.key();
      }
    }

    @Override
    public boolean hasNext() {
      return a != null || b != null;
    }

    @Override
    public T next() {
      T result;
      if (lessThan(ak, bk, a, b)) {
        result = a;
        if (inputA.hasNext()) {
          a = inputA.next();
          ak = a.key();
        } else {
          a = null;
          ak = Long.MAX_VALUE;
        }
      } else if (bk == Long.MAX_VALUE) {
        throw new NoSuchElementException();
      } else {
        result = b;
        if (inputB.hasNext()) {
          b = inputB.next();
          bk = b.key();
        } else {
          b = null;
          bk = Long.MAX_VALUE;
        }
      }
      return result;
    }

    private boolean lessThan(long ak, long bk, T a, T b) {
      return ak < bk || (ak == bk && a != null && b != null && tieBreaker.compare(a, b) < 0);
    }
  }

  private static class ThreeWayMerge<T extends HasLongSortKey> implements Iterator<T> {

    private final Comparator<T> tieBreaker;
    T a, b, c;
    long ak = Long.MAX_VALUE, bk = Long.MAX_VALUE, ck = Long.MAX_VALUE;
    final Iterator<T> inputA, inputB, inputC;

    ThreeWayMerge(Iterator<T> inputA, Iterator<T> inputB, Iterator<T> inputC, Comparator<T> tieBreaker) {
      this.tieBreaker = tieBreaker;
      this.inputA = inputA;
      this.inputB = inputB;
      this.inputC = inputC;
      if (inputA.hasNext()) {
        a = inputA.next();
        ak = a.key();
      }
      if (inputB.hasNext()) {
        b = inputB.next();
        bk = b.key();
      }
      if (inputC.hasNext()) {
        c = inputC.next();
        ck = c.key();
      }
    }

    @Override
    public boolean hasNext() {
      return a != null || b != null || c != null;
    }

    @Override
    public T next() {
      T result;
      // use at most 2 comparisons to get the next item
      if (lessThan(ak, bk, a, b)) {
        if (lessThan(ak, ck, a, c)) {
          // ACB / ABC
          result = a;
          if (inputA.hasNext()) {
            a = inputA.next();
            ak = a.key();
          } else {
            a = null;
            ak = Long.MAX_VALUE;
          }
        } else {
          // CBA
          result = c;
          if (inputC.hasNext()) {
            c = inputC.next();
            ck = c.key();
          } else {
            c = null;
            ck = Long.MAX_VALUE;
          }
        }
      } else if (lessThan(ck, bk, c, b)) {
        // CAB
        result = c;
        if (inputC.hasNext()) {
          c = inputC.next();
          ck = c.key();
        } else {
          c = null;
          ck = Long.MAX_VALUE;
        }
      } else if (bk == Long.MAX_VALUE) {
        throw new NoSuchElementException();
      } else {
        // BAC / BCA
        result = b;
        if (inputB.hasNext()) {
          b = inputB.next();
          bk = b.key();
        } else {
          b = null;
          bk = Long.MAX_VALUE;
        }
      }
      return result;
    }

    private boolean lessThan(long ak, long bk, T a, T b) {
      return ak < bk || (ak == bk && lessThanCmp(a, b, tieBreaker));
    }
  }

  private static <T> boolean lessThanCmp(T a, T b, Comparator<T> tieBreaker) {
    // nulls go at the end
    if (a == null) {
      return false;
    } else if (b == null) {
      return true;
    } else {
      return tieBreaker.compare(a, b) < 0;
    }
  }

  private static class KWayMerge<T extends HasLongSortKey> implements Iterator<T> {
    private final T[] items;
    private final Iterator<T>[] iterators;
    private final LongMinHeap heap;

    @SuppressWarnings("unchecked")
    KWayMerge(List<? extends Iterator<T>> inputIterators, Comparator<T> tieBreaker) {
      this.iterators = new Iterator[inputIterators.size()];
      this.items = (T[]) new HasLongSortKey[inputIterators.size()];
      this.heap = LongMinHeap.newArrayHeap(inputIterators.size(), (a, b) -> tieBreaker.compare(items[a], items[b]));
      int outIdx = 0;
      for (Iterator<T> iter : inputIterators) {
        if (iter.hasNext()) {
          var item = iter.next();
          items[outIdx] = item;
          iterators[outIdx] = iter;
          heap.push(outIdx++, item.key());
        }
      }
    }

    @Override
    public boolean hasNext() {
      return !heap.isEmpty();
    }

    @Override
    public T next() {
      if (heap.isEmpty()) {
        throw new NoSuchElementException();
      }
      int id = heap.peekId();
      T result = items[id];
      Iterator<T> iterator = iterators[id];
      if (iterator.hasNext()) {
        T next = iterator.next();
        items[id] = next;
        heap.updateHead(next.key());
      } else {
        items[id] = null;
        heap.poll();
      }
      return result;
    }
  }
}
