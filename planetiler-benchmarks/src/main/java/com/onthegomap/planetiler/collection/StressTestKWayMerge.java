package com.onthegomap.planetiler.collection;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class StressTestKWayMerge {

  public static void main(String[] args) throws InterruptedException {
    for (int i = 1; i < 20; i++) {
      test(i, 100_000, 200_000);
    }
    for (int i = 50; i <= 500; i += 50) {
      test(i, 10_000, 20_000);
    }
    test(5_000, 1000, 2000);
  }

  private static void test(int n, long items, long maxKey) throws InterruptedException {
    System.out.println("test(" + n + ")");
    var random = new Random(0);
    List<List<SortableFeature>> featureLists = new ArrayList<>();
    ExecutorService executorService = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
    for (int i = 0; i < n; i++) {
      List<SortableFeature> list = new ArrayList<>();
      featureLists.add(list);
      for (int j = 0; j < items; j++) {
        byte[] bytes = new byte[random.nextInt(1, 10)];
        random.nextBytes(bytes);
        list.add(new SortableFeature(random.nextLong(maxKey), bytes));
      }
      executorService.submit(() -> list.sort(Comparator.naturalOrder()));
    }
    executorService.shutdown();
    executorService.awaitTermination(1, TimeUnit.DAYS);


    var iter =
      LongMerger.mergeIterators(featureLists.stream().map(List::iterator).toList(), SortableFeature.COMPARE_BYTES);
    var last = iter.next();
    int i = 1;
    while (iter.hasNext()) {
      i++;
      var item = iter.next();
      if (last.compareTo(item) > 0) {
        System.err
          .println("items out of order lists=" + n + " last=" + last + " item=" + item + " i=" + i);
        return;
      }
      last = item;
    }
  }
}
