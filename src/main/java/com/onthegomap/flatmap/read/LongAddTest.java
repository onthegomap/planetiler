package com.onthegomap.flatmap.read;

import com.graphhopper.util.StopWatch;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

public class LongAddTest {

  private static void time(Runnable r) {
    StopWatch w = new StopWatch().start();
    r.run();
    System.err.println(w.stop());
  }

  public static void main(String[] args) {
    time(() -> {
      long count = 0;
      for (long i = 0; i < 1_000_000_000L; i++) {
        count++;
      }
      System.err.println(count);
    });
    time(() -> {
      LongAdder adder = new LongAdder();
      for (long i = 0; i < 1_000_000_000L; i++) {
        adder.increment();
      }
      System.err.println(adder.longValue());
    });
    time(() -> {
      AtomicLong adder = new AtomicLong();
      for (long i = 0; i < 1_000_000_000L; i++) {
        adder.incrementAndGet();
      }
      System.err.println(adder.longValue());
    });
  }
}
