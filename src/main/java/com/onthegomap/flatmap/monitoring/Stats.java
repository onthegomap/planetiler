package com.onthegomap.flatmap.monitoring;

import static io.prometheus.client.Collector.NANOSECONDS_PER_SECOND;

import com.onthegomap.flatmap.MemoryEstimator;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

public interface Stats extends AutoCloseable {

  void time(String name, Runnable task);

  default void printSummary() {
    timers().printSummary();
  }

  Timers.Finishable startTimer(String name);

  default void gauge(String name, Number value) {
    gauge(name, () -> value);
  }

  void gauge(String name, Supplier<Number> value);

  void emittedFeature(int z, String layer, int coveringTiles);

  void encodedTile(int zoom, int length);

  void wroteTile(int zoom, int bytes);

  Timers timers();

  void monitorFile(String features, Path featureDbPath);

  void monitorInMemoryObject(String name, MemoryEstimator.HasEstimate heapObject);

  void counter(String name, Supplier<Number> supplier);

  default StatCounter longCounter(String name) {
    StatCounter.AtomicCounter counter = new StatCounter.AtomicCounter();
    counter(name, counter::get);
    return counter;
  }

  default StatCounter nanoCounter(String name) {
    StatCounter.AtomicCounter counter = new StatCounter.AtomicCounter();
    counter(name, () -> counter.get() / NANOSECONDS_PER_SECOND);
    return counter;
  }

  interface StatCounter {

    void inc(long v);

    default void inc() {
      inc(1);
    }

    class NoopCounter implements StatCounter {

      @Override
      public void inc(long v) {
      }
    }

    class AtomicCounter implements StatCounter {

      private final AtomicLong counter = new AtomicLong(0);

      @Override
      public void inc(long v) {
        counter.addAndGet(v);
      }

      public long get() {
        return counter.get();
      }
    }
  }

  class InMemory implements Stats {

    private static final StatCounter NOOP_COUNTER = new StatCounter.NoopCounter();
    private final Timers timers = new Timers();

    @Override
    public void time(String name, Runnable task) {
      timers.time(name, task);
    }

    @Override
    public Timers.Finishable startTimer(String name) {
      return timers.startTimer(name);
    }

    @Override
    public void encodedTile(int zoom, int length) {

    }

    @Override
    public void wroteTile(int zoom, int bytes) {
    }

    @Override
    public Timers timers() {
      return timers;
    }

    @Override
    public void monitorFile(String features, Path featureDbPath) {
    }

    @Override
    public void monitorInMemoryObject(String name, MemoryEstimator.HasEstimate heapObject) {
    }

    @Override
    public void counter(String name, Supplier<Number> supplier) {
    }

    @Override
    public StatCounter longCounter(String name) {
      return NOOP_COUNTER;
    }

    @Override
    public StatCounter nanoCounter(String name) {
      return NOOP_COUNTER;
    }

    @Override
    public void gauge(String name, Supplier<Number> value) {
    }

    @Override
    public void emittedFeature(int z, String layer, int coveringTiles) {
    }

    @Override
    public void close() throws Exception {

    }
  }
}
