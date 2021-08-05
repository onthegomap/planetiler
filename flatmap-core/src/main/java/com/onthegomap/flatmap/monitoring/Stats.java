package com.onthegomap.flatmap.monitoring;

import static io.prometheus.client.Collector.NANOSECONDS_PER_SECOND;

import com.onthegomap.flatmap.MemoryEstimator;
import java.nio.file.Path;
import java.util.Map;
import java.util.function.Supplier;

public interface Stats extends AutoCloseable {

  default void printSummary() {
    timers().printSummary();
  }

  Timers.Finishable startTimer(String name);

  default void gauge(String name, Number value) {
    gauge(name, () -> value);
  }

  void gauge(String name, Supplier<Number> value);

  void emittedFeatures(int z, String layer, int coveringTiles);

  void wroteTile(int zoom, int bytes);

  Timers timers();

  void monitorFile(String features, Path featureDbPath);

  void monitorInMemoryObject(String name, MemoryEstimator.HasEstimate heapObject);

  void counter(String name, Supplier<Number> supplier);

  default Counter.MultiThreadCounter longCounter(String name) {
    Counter.MultiThreadCounter counter = Counter.newMultiThreadCounter();
    counter(name, counter::get);
    return counter;
  }

  default Counter.MultiThreadCounter nanoCounter(String name) {
    Counter.MultiThreadCounter counter = Counter.newMultiThreadCounter();
    counter(name, () -> counter.get() / NANOSECONDS_PER_SECOND);
    return counter;
  }

  void counter(String name, String label, Supplier<Map<String, Counter.Readable>> values);

  void processedElement(String elemType, String layer);

  void dataError(String stat);

  static Stats inMemory() {
    return new InMemory();
  }

  class InMemory implements Stats {

    private final Timers timers = new Timers();

    @Override
    public Timers.Finishable startTimer(String name) {
      return timers.startTimer(name);
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
    public Counter.MultiThreadCounter longCounter(String name) {
      return Counter.newMultiThreadCounter();
    }

    @Override
    public Counter.MultiThreadCounter nanoCounter(String name) {
      return Counter.newMultiThreadCounter();
    }

    @Override
    public void counter(String name, String label, Supplier<Map<String, Counter.Readable>> values) {
    }

    @Override
    public void processedElement(String elemType, String layer) {
    }

    @Override
    public void dataError(String stat) {
    }

    @Override
    public void gauge(String name, Supplier<Number> value) {
    }

    @Override
    public void emittedFeatures(int z, String layer, int coveringTiles) {
    }

    @Override
    public void close() throws Exception {

    }
  }
}
