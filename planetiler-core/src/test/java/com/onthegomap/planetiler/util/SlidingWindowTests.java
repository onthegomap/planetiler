package com.onthegomap.planetiler.util;

import static org.junit.jupiter.api.Assertions.assertFalse;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

class SlidingWindowTests {
  @Test
  @Timeout(10)
  void testSlidingWindow() throws InterruptedException {
    var slidingWindow = new SlidingWindow(5);
    var latch1 = new CountDownLatch(1);
    var latch2 = new CountDownLatch(1);
    Thread t1 = new Thread(() -> {
      latch1.countDown();
      slidingWindow.waitUntilInsideWindow(9);
      latch2.countDown();
    });
    t1.start();
    latch1.await();
    assertFalse(latch2.await(100, TimeUnit.MILLISECONDS));
    slidingWindow.advanceTail(4);
    assertFalse(latch2.await(100, TimeUnit.MILLISECONDS));
    slidingWindow.advanceTail(5);
    latch2.await();
    t1.join();
  }
}
