package com.onthegomap.planetiler.stats;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

class CounterTest {

  @Test
  void testSingleThreadedCounter() {
    var counter = Counter.newSingleThreadCounter();
    assertEquals(0, counter.get());

    counter.inc();
    assertEquals(1, counter.get());
    counter.incBy(2);
    assertEquals(3, counter.get());
    counter.incBy(-1);
    assertEquals(2, counter.get());
  }

  @Test
  void testMultiThreadedCounter() throws InterruptedException {
    var counter = Counter.newMultiThreadCounter();
    Thread t1 = new Thread(() -> {
      counter.incBy(1);
      counter.incBy(1);
    });
    t1.start();
    Thread t2 = new Thread(() -> counter.incBy(1));
    t2.start();
    t1.join();
    t2.join();
    assertEquals(3, counter.get());
  }

  @Test
  void testMultiThreadedSubCounter() throws InterruptedException {
    var counter = Counter.newMultiThreadCounter();
    Thread t1 = new Thread(() -> {
      var subCounter = counter.counterForThread();
      subCounter.incBy(1);
      subCounter.incBy(1);
    });
    t1.start();
    Thread t2 = new Thread(() -> {
      var subCounter = counter.counterForThread();
      subCounter.incBy(1);
    });
    t2.start();
    t1.join();
    t2.join();
    assertEquals(3, counter.get());
  }
}
