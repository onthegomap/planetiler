package com.onthegomap.planetiler.benchmarks;

import com.onthegomap.planetiler.geo.LineSplitter;
import java.util.concurrent.ThreadLocalRandom;
import org.locationtech.jts.util.GeometricShapeFactory;

public class BenchmarkLineSplitter {

  public static void main(String[] args) {
    for (int i = 0; i < 10; i++) {
      System.err.println(
        "reused:\t" +
          timeReused(10, 1_000_000) + "\t" +
          timeReused(100, 100_000) + "\t" +
          timeReused(1_000, 10_000) +
          "\t!reused\t" +
          timeNotReused(10, 1_000_000) + "\t" +
          timeNotReused(100, 100_000) + "\t" +
          timeNotReused(1_000, 10_000) +
          "\tcacheable\t" +
          timeCacheable(10, 1_000_000) + "\t" +
          timeCacheable(100, 100_000) + "\t" +
          timeCacheable(1_000, 10_000));
    }
  }

  private static long timeCacheable(int points, int iters) {
    var fact = new GeometricShapeFactory();
    fact.setNumPoints(points);
    fact.setWidth(10);
    var shape = fact.createArc(0, Math.PI);
    long start = System.currentTimeMillis();
    LineSplitter splitter = new LineSplitter(shape);
    var random = ThreadLocalRandom.current();
    for (int i = 0; i < iters; i++) {
      int a = random.nextInt(0, 90);
      int b = random.nextInt(a + 2, 100);
      splitter.get(a / 100d, b / 100d);
    }
    return System.currentTimeMillis() - start;
  }

  private static long timeReused(int points, int iters) {
    var fact = new GeometricShapeFactory();
    fact.setNumPoints(points);
    fact.setWidth(10);
    var shape = fact.createArc(0, Math.PI);
    long start = System.currentTimeMillis();
    LineSplitter splitter = new LineSplitter(shape);
    var random = ThreadLocalRandom.current();
    for (int i = 0; i < iters; i++) {
      var a = random.nextDouble(0, 1);
      var b = random.nextDouble(a, 1);
      splitter.get(a, b);
    }
    return System.currentTimeMillis() - start;
  }

  private static long timeNotReused(int points, int iters) {
    var fact = new GeometricShapeFactory();
    fact.setNumPoints(points);
    fact.setWidth(10);
    var shape = fact.createArc(0, Math.PI);
    long start = System.currentTimeMillis();
    var random = ThreadLocalRandom.current();
    for (int i = 0; i < iters; i++) {
      LineSplitter splitter = new LineSplitter(shape);
      var a = random.nextDouble(0, 1);
      var b = random.nextDouble(a, 1);
      splitter.get(a, b);
    }
    return System.currentTimeMillis() - start;
  }
}
