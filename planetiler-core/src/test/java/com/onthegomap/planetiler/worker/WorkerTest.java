package com.onthegomap.planetiler.worker;

import static org.junit.jupiter.api.Assertions.assertThrows;

import com.onthegomap.planetiler.ExpectedException;
import com.onthegomap.planetiler.stats.Stats;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

class WorkerTest {

  @Test
  @Timeout(10)
  void testExceptionHandled() {
    var worker = new Worker("prefix", Stats.inMemory(), 4, workerNum -> {
      if (workerNum == 1) {
        throw new ExpectedException();
      } else {
        Thread.sleep(5000);
      }
    });
    assertThrows(RuntimeException.class, worker::await);
  }
}
