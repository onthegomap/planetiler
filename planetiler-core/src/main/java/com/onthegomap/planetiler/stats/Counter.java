package com.onthegomap.planetiler.stats;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;

/**
 * A {@code long} value that can go up and down.
 *
 * <p>Incrementing an {@link AtomicLong} is fast from a single thread, but can be a bottleneck when
 * multiple threads try to increment it at the same time, so this utility provides a {@link
 * MultiThreadCounter} that gives out counters to each thread and adds up the total on read.
 */
public interface Counter {

  default void inc() {
    incBy(1);
  }

  void incBy(long value);

  /** A counter that lets clients get the current value. */
  interface Readable extends Counter, LongSupplier {

    long get();

    @Override
    default long getAsLong() {
      return get();
    }
  }

  /**
   * Returns a counter that is optimized for updates from a single thread, but still thread safe for
   * reads and writes from multiple threads.
   */
  static Readable newSingleThreadCounter() {
    return new SingleThreadCounter();
  }

  /**
   * Returns a counter optimized for updates from multiple threads, and infrequent reads from a
   * different thread.
   */
  static MultiThreadCounter newMultiThreadCounter() {
    return new MultiThreadCounter();
  }

  /**
   * Counter optimized for updates from a single thread, but still safe for reads and writes from
   * multiple threads.
   */
  class SingleThreadCounter implements Readable {

    private SingleThreadCounter() {}

    private final AtomicLong counter = new AtomicLong(0);

    @Override
    public void incBy(long value) {
      counter.addAndGet(value);
    }

    @Override
    public long get() {
      return counter.get();
    }
  }

  /**
   * Counter optimized for updates from a multiple threads and infrequent reads from other threads.
   *
   * <p>Threads should get a counter to update using {@link #counterForThread()} once and update the
   * that instead of calling {@link #inc()} or {@link #incBy(long)} to avoid the cost of a
   * thread-local lookup.
   */
  class MultiThreadCounter implements Readable {

    private MultiThreadCounter() {}

    // keep track of all counters that have been handed out to threads so far
    // and on read, add up the counts from each
    private final List<SingleThreadCounter> all = new CopyOnWriteArrayList<>();
    private final ThreadLocal<SingleThreadCounter> thread =
        ThreadLocal.withInitial(
            () -> {
              SingleThreadCounter counter = new SingleThreadCounter();
              all.add(counter);
              return counter;
            });

    /** Don't use this, get a counter from {@link #counterForThread()} once and use that. */
    @Override
    public void incBy(long value) {
      thread.get().incBy(value);
    }

    /**
     * Returns the counter for this thread, so it can be cached to avoid subsequent thread local
     * lookups.
     */
    public Counter counterForThread() {
      return thread.get();
    }

    @Override
    public long get() {
      return all.stream().mapToLong(SingleThreadCounter::get).sum();
    }
  }
}
