package com.onthegomap.planetiler.collection;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.function.Supplier;

public class LongMerger {
  private LongMerger() {}

  public static <T extends HasLongSortKey> Iterator<T> mergeSuppliers(List<? extends Supplier<T>> suppliers) {
    return mergeIterators(suppliers.stream().map(SupplierIterator::new).toList());
  }

  public static <T extends HasLongSortKey> Iterator<T> mergeIterators(List<? extends Iterator<T>> iterators) {
    return switch (iterators.size()) {
      case 0 -> Collections.emptyIterator();
      case 1 -> iterators.get(0);
      case 2 -> new TwoWayMerge<>(iterators.get(0), iterators.get(1));
      case 3 -> new ThreeWayMerge<>(iterators.get(0), iterators.get(1), iterators.get(2));
      default -> new KWayMerge<>(iterators);
    };
  }

  private static class TwoWayMerge<T extends HasLongSortKey> implements Iterator<T> {
    T a, b;
    long ak = Long.MAX_VALUE, bk = Long.MAX_VALUE;
    final Iterator<T> inputA, inputB;

    TwoWayMerge(Iterator<T> inputA, Iterator<T> inputB) {
      this.inputA = inputA;
      this.inputB = inputB;
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
      if (ak < bk) {
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
  }

  private static class ThreeWayMerge<T extends HasLongSortKey> implements Iterator<T> {
    T a, b, c;
    long ak = Long.MAX_VALUE, bk = Long.MAX_VALUE, ck = Long.MAX_VALUE;
    final Iterator<T> inputA, inputB, inputC;

    ThreeWayMerge(Iterator<T> inputA, Iterator<T> inputB, Iterator<T> inputC) {
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
      if (ak < bk) {
        if (ak < ck) {
          result = a;
          if (inputA.hasNext()) {
            a = inputA.next();
            ak = a.key();
          } else {
            a = null;
            ak = Long.MAX_VALUE;
          }
        } else {
          result = c;
          if (inputC.hasNext()) {
            c = inputC.next();
            ck = c.key();
          } else {
            c = null;
            ck = Long.MAX_VALUE;
          }
        }
      } else if (ck < bk) {
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
  }

  private static class KWayMerge<T extends HasLongSortKey> implements Iterator<T> {
    private final List<T> items;
    private final List<Iterator<T>> suppliers;
    private final LongMinHeap heap;

    KWayMerge(List<? extends Iterator<T>> iterators) {
      this.suppliers = new ArrayList<>();
      this.items = new ArrayList<>();
      this.heap = LongMinHeap.newArrayHeap(iterators.size());
      int outIdx = 0;
      for (Iterator<T> iter : iterators) {
        if (iter.hasNext()) {
          var item = iter.next();
          items.add(item);
          suppliers.add(iter);
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
      T result = items.get(id);
      Iterator<T> iterator = suppliers.get(id);
      if (iterator.hasNext()) {
        T next = iterator.next();
        items.set(id, next);
        heap.updateHead(next.key());
      } else {
        items.set(id, null);
        heap.poll();
      }
      return result;
    }
  }
}
