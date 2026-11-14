package com.onthegomap.planetiler.benchmarks;

import com.onthegomap.planetiler.util.Scales;
import java.util.function.DoubleUnaryOperator;
import java.util.function.Supplier;

public class BenchmarkInterpolator {
  private static double sum = 0;

  public static void main(String[] args) {
    long times = 10_000_000;
    benchmarkInterpolator("linear", times, Scales::linear);
    benchmarkInterpolator("sqrt", times, Scales::sqrt);
    benchmarkInterpolator("pow2", times, () -> Scales.power(2));
    benchmarkInterpolator("pow10", times, () -> Scales.power(1));
    benchmarkInterpolator("log", times, () -> Scales.log(10));
    benchmarkInterpolator("log2", times, () -> Scales.log(2));
    System.err.println(sum);
  }

  private static void benchmarkInterpolator(String name, long times, Supplier<Scales.DoubleContinuous> get) {
    benchmarkAndInverted(name + "_2", 1, 2, times, () -> get.get().put(1, 1d).put(2, 2d));
    benchmarkAndInverted(name + "_3", 1, 2, times, () -> get.get().put(1, 1d).put(1.5, 2d).put(2, 3d));
  }

  private static void benchmarkAndInverted(String name, double start, double end, long steps,
    Supplier<Scales.DoubleContinuous> build) {
    benchmark(name + "_f", start, end, steps, build::get);
    benchmark(name + "_i", start, end, steps, () -> build.get().invert());
  }

  private static void benchmark(String name, double start, double end, long steps,
    Supplier<DoubleUnaryOperator> build) {
    double delta = (end - start) / steps;
    double x = start;
    double result = 0;
    for (long i = 0; i < steps / 10_000; i++) {
      result += build.get().applyAsDouble(x += delta);
    }
    x = start;
    long a = System.currentTimeMillis();
    for (long i = 0; i < steps; i++) {
      result += build.get().applyAsDouble(x += delta);
    }
    x = start;
    var preBuilt = build.get();
    long b = System.currentTimeMillis();
    for (long i = 0; i < steps; i++) {
      result += preBuilt.applyAsDouble(x += delta);
    }
    long c = System.currentTimeMillis();
    sum += result;
    System.err.println(name + "\t" + (b - a) + "\t" + (c - b));
  }
}
