package com.onthegomap.planetiler.worker;

import static org.junit.jupiter.api.Assertions.assertThrows;

import com.onthegomap.planetiler.ExpectedException;
import com.onthegomap.planetiler.stats.Stats;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

public class WorkerTest {

  @Test
  @Timeout(10)
  public void testExceptionHandled() {
    AtomicInteger counter = new AtomicInteger(0);
    var worker = new Worker("prefix", Stats.inMemory(), 4, () -> {
      if (counter.incrementAndGet() == 1) {
        throw new ExpectedException();
      } else {
        Thread.sleep(5000);
      }
    });
    assertThrows(RuntimeException.class, worker::await);
  }
}
