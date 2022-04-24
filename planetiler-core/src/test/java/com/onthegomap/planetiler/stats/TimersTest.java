package com.onthegomap.planetiler.stats;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class TimersTest {

  @Test
  void testTimers() {
    Timers timers = new Timers();
    assertTrue(timers.all().isEmpty());
    timers.printSummary();

    var finish = timers.startTimer("task2");
    assertTrue(timers.all().get("task2").timer().running());
    finish.stop();
    assertFalse(timers.all().get("task2").timer().running());

    timers.printSummary();
  }
}
