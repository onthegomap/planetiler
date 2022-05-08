package com.onthegomap.planetiler.collection;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Random;
import java.util.function.IntFunction;
import java.util.stream.IntStream;

/**
 * Performance tests for {@link LongMinHeap} implementations.
 *
 * Times how long it takes to merge N sorted lists of random elements.
 */
public class BenchmarkHeap {
  public static void main(String[] args) {
    for (int i = 0; i < 3; i++) {
      System.err.println();
      testMinHeap("binary", LongMinHeap::newBinaryArrayHeap);
      testMinHeap("quaternary", LongMinHeap::newQuaternaryArrayHeap);
      System.err.println(String.join("\t",
        "priorityqueue",
        Long.toString(testPriorityQueue(10).toMillis()),
        Long.toString(testPriorityQueue(100).toMillis()),
        Long.toString(testPriorityQueue(1_000).toMillis()),
        Long.toString(testPriorityQueue(10_000).toMillis())));
    }
  }

  private static void testMinHeap(String name, IntFunction<LongMinHeap> constructor) {
    System.err.println(String.join("\t",
      name,
      Long.toString(testUpdates(10, constructor).toMillis()),
      Long.toString(testUpdates(100, constructor).toMillis()),
      Long.toString(testUpdates(1_000, constructor).toMillis()),
      Long.toString(testUpdates(10_000, constructor).toMillis())));
  }

  private static final Map<Integer, long[][]> cache = new HashMap<>();

  private static long[][] getVals(int size) {
    return cache.computeIfAbsent(size, s -> {
      int num = 50_000_000;
      var random = new Random(0);
      return IntStream.range(0, size)
        .mapToObj(i -> random
          .longs(0, 1_000_000_000)
          .limit(num / size)
          .sorted()
          .toArray()
        ).toArray(long[][]::new);
    });
  }

  private static Duration testUpdates(int size, IntFunction<LongMinHeap> heapFn) {
    int[] indexes = new int[size];
    long[][] vals = getVals(size);
    var heap = heapFn.apply(size);
    for (int i = 0; i < size; i++) {
      heap.push(i, vals[i][indexes[i]++]);
    }
    var start = System.nanoTime();
    while (!heap.isEmpty()) {
      int id = heap.peekId();
      int index = indexes[id]++;
      long[] valList = vals[id];
      if (index < valList.length) {
        heap.updateHead(valList[index]);
      } else {
        heap.poll();
      }
    }
    return Duration.ofNanos(System.nanoTime() - start);
  }

  static class Item implements Comparable<Item> {
    long value;
    int id;

    @Override
    public int compareTo(Item o) {
      return Long.compare(value, o.value);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      Item item = (Item) o;

      return value == item.value;
    }

    @Override
    public int hashCode() {
      return (int) (value ^ (value >>> 32));
    }
  }

  private static Duration testPriorityQueue(int size) {
    long[][] vals = getVals(size);
    int[] indexes = new int[size];
    PriorityQueue<Item> heap = new PriorityQueue<>();
    for (int i = 0; i < size; i++) {
      Item item = new Item();
      item.id = i;
      item.value = vals[i][indexes[i]++];
      heap.offer(item);
    }
    var start = System.nanoTime();
    while (!heap.isEmpty()) {
      var item = heap.poll();
      int index = indexes[item.id]++;
      long[] valList = vals[item.id];
      if (index < valList.length) {
        item.value = valList[index];
        heap.offer(item);
      }
    }
    return Duration.ofNanos(System.nanoTime() - start);
  }
}
