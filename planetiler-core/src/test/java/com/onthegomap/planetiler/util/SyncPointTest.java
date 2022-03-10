package com.onthegomap.planetiler.util;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

public class SyncPointTest {
  @Test
  @Timeout(10)
  public void testEmptySyncPoint() {
    var syncPoint = new SyncPoint(0);
    var finisher1 = syncPoint.newFinisher();
    var finisher2 = syncPoint.newFinisher();
    assertFalse(finisher1.isFinished());
    finisher1.finish();
    assertTrue(finisher1.isFinished());
    assertFalse(finisher2.isFinished());
    finisher2.await(); // noop
  }

  @Test
  @Timeout(10)
  public void testSyncPoint() throws InterruptedException, BrokenBarrierException {
    var syncPoint = new SyncPoint(2);
    var finisher1 = syncPoint.newFinisher();
    var finisher2 = syncPoint.newFinisher();
    assertThrows(IllegalStateException.class, syncPoint::newFinisher);
    var latch1 = new CyclicBarrier(2);
    var latch2 = new CountDownLatch(1);
    var t = new Thread(() -> {
      try {
        latch1.await();
        finisher1.finish();
        finisher1.await();
        latch2.countDown();
      } catch (InterruptedException | BrokenBarrierException e) {
        e.printStackTrace();
      }
    });
    t.start();
    latch1.await();
    assertFalse(latch2.await(100, TimeUnit.MILLISECONDS));
    assertFalse(finisher2.isFinished());
    finisher2.finish();
    assertTrue(finisher2.isFinished());
    latch2.await();
    t.join();

    finisher2.finish();
    finisher2.finish();
    finisher2.finish();
    finisher2.await();
    finisher2.await();
  }
}
