package com.onthegomap.flatmap.stats;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

public class TimerTest {

  @Test
  public void testTimer() {
    Timer timer = new Timer().start();
    ProcessTime elapsed1 = timer.elapsed();
    ProcessTime elapsed2 = timer.stop().elapsed();
    ProcessTime elapsed3 = timer.elapsed();

    assertEquals(elapsed2.wall(), elapsed3.wall());
    assertLessThan(elapsed1.wall(), elapsed2.wall());
    assertFalse(elapsed3.cpu().isEmpty(), "no CPU time");
  }

  private <T extends Comparable<T>> void assertLessThan(T a, T b) {
    assertTrue(a.compareTo(b) < 0, a + " is not less than " + b);
  }
}
