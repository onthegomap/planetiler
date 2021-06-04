package com.onthegomap.flatmap.monitoring;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
}
