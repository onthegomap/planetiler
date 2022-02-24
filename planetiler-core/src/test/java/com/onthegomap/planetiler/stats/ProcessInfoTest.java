package com.onthegomap.planetiler.stats;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import org.junit.jupiter.api.Test;

public class ProcessInfoTest {

  @Test
  public void testGC() {
    assertTrue(ProcessInfo.getGcTime().toNanos() >= 0);
  }

  @Test
  public void testCPU() {
    assertFalse(ProcessInfo.getProcessCpuTime().isEmpty());
  }

  @Test
  public void testThreads() {
    assertFalse(ProcessInfo.getThreadStats().isEmpty());
  }

  @Test
  public void testAdd() {
    var a = new ProcessInfo.ThreadState("", Duration.ofSeconds(1), Duration.ofSeconds(2), Duration.ofSeconds(3),
      Duration.ofSeconds(4), -1);
    var b = new ProcessInfo.ThreadState("", Duration.ofSeconds(5), Duration.ofSeconds(6), Duration.ofSeconds(7),
      Duration.ofSeconds(8), -1);
    var sum = a.plus(b);
    assertEquals(Duration.ofSeconds(6), sum.cpuTime());
    assertEquals(Duration.ofSeconds(8), sum.userTime());
    assertEquals(Duration.ofSeconds(10), sum.waiting());
    assertEquals(Duration.ofSeconds(12), sum.blocking());
  }
}
