package com.onthegomap.planetiler.collection;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.PriorityQueue;
import java.util.Random;
import java.util.function.IntFunction;
import java.util.stream.IntStream;

/**
 * Performance tests for {@link SortableFeatureMinHeap} implementations.
 *
 * Times how long it takes to merge N sorted lists of random elements.
 */
public class BenchmarkKWayMerge {

  public static void main(String[] args) {
    for (int i = 0; i < 4; i++) {
      System.err.println();
      testMinHeap("quaternary", SortableFeatureMinHeap::newArrayHeap);
      System.err.println(String.join("\t",
        "priorityqueue",
        Long.toString(testPriorityQueue(10).toMillis()),
        Long.toString(testPriorityQueue(100).toMillis()),
        Long.toString(testPriorityQueue(1_000).toMillis()),
        Long.toString(testPriorityQueue(10_000).toMillis())));
    }
  }

  private static void testMinHeap(String name, IntFunction<SortableFeatureMinHeap> constructor) {
    System.err.println(String.join("\t",
      name,
      Long.toString(testUpdates(10, constructor).toMillis()),
      Long.toString(testUpdates(100, constructor).toMillis()),
      Long.toString(testUpdates(1_000, constructor).toMillis()),
      Long.toString(testUpdates(10_000, constructor).toMillis())));
  }

  private static final Random random = new Random();

  final static ByteBuffer byteBuffer = ByteBuffer.allocate(Long.BYTES);

  private static SortableFeature newVal(long i) {
    byteBuffer.clear().putLong(i);
    return new SortableFeature(i, byteBuffer.array());
  }

  private static SortableFeature[][] getVals(int size) {
    int num = 10_000_000;
    return IntStream.range(0, size)
      .mapToObj(i -> random
        .longs(0, 1_000_000_000)
        .limit(num / size)
        .sorted()
        .mapToObj(BenchmarkKWayMerge::newVal)
        .toArray(SortableFeature[]::new)
      ).toArray(SortableFeature[][]::new);
  }

  private static Duration testUpdates(int size, IntFunction<SortableFeatureMinHeap> heapFn) {
    int[] indexes = new int[size];
    SortableFeature[][] vals = getVals(size);
    var heap = heapFn.apply(size);
    for (int i = 0; i < size; i++) {
      heap.push(i, vals[i][indexes[i]++]);
    }
    var start = System.nanoTime();
    while (!heap.isEmpty()) {
      int id = heap.peekId();
      int index = indexes[id]++;
      SortableFeature[] valList = vals[id];
      if (index < valList.length) {
        heap.updateHead(valList[index]);
      } else {
        heap.poll();
      }
    }
    return Duration.ofNanos(System.nanoTime() - start);
  }

  private static Duration testPriorityQueue(int size) {
    SortableFeature[][] vals = getVals(size);
    int[] indexes = new int[size];
    PriorityQueue<SortableFeature> heap = new PriorityQueue<>();
    for (int i = 0; i < size; i++) {
      byteBuffer.clear().putLong(i);
      SortableFeature temp = vals[i][indexes[i]++];
      SortableFeature item = new SortableFeature(temp.key(), byteBuffer.array());
      heap.offer(item);
    }
    var start = System.nanoTime();
    while (!heap.isEmpty()) {
      var item = heap.poll();
      int id = (int) byteBuffer.clear().put(item.value()).rewind().getLong();
      int index = indexes[id]++;
      SortableFeature[] valList = vals[id];
      if (index < valList.length) {
        item = valList[index];
        heap.offer(item);
      }
    }
    return Duration.ofNanos(System.nanoTime() - start);
  }
}
