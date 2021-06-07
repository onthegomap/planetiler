package com.onthegomap.flatmap.monitoring;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;

public interface Counter {

  void inc();

  void incBy(long value);

  interface Readable extends Counter, LongSupplier {

    long get();

    @Override
    default long getAsLong() {
      return get();
    }
  }

  static Readable newSingleThreadCounter() {
    return new SingleThreadCounter();
  }

  static MultiThreadCounter newMultiThreadCounter() {
    return new MultiThreadCounter();
  }

  static Counter.Readable noop() {
    return NoopCoounter.instance;
  }

  class SingleThreadCounter implements Readable {

    private SingleThreadCounter() {
    }

    private final AtomicLong counter = new AtomicLong(0);

    @Override
    public void inc() {
      counter.incrementAndGet();
    }

    @Override
    public void incBy(long value) {
      counter.addAndGet(value);
    }

    @Override
    public long get() {
      return counter.get();
    }
  }

  class MultiThreadCounter implements Readable {

    private MultiThreadCounter() {
    }

    private final List<SingleThreadCounter> all = Collections.synchronizedList(new ArrayList<>());
    private final ThreadLocal<SingleThreadCounter> thread = ThreadLocal.withInitial(() -> {
      SingleThreadCounter counter = new SingleThreadCounter();
      all.add(counter);
      return counter;
    });

    @Override
    public void inc() {
      thread.get().inc();
    }

    @Override
    public void incBy(long value) {
      thread.get().incBy(value);
    }

    public Counter counterForThread() {
      return thread.get();
    }

    @Override
    public long get() {
      return all.stream().mapToLong(SingleThreadCounter::get).sum();
    }
  }

  class NoopCoounter implements Counter.Readable {

    private static final NoopCoounter instance = new NoopCoounter();

    @Override
    public void inc() {
    }

    @Override
    public void incBy(long value) {
    }

    @Override
    public long get() {
      return 0;
    }
  }
}
