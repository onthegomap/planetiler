package com.onthegomap.planetiler.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.junit.jupiter.api.Test;

class LogUtilTest {
  @Test
  void testStageHandling() {
    assertNull(LogUtil.getStage());
    LogUtil.setStage("test");
    assertEquals("test", LogUtil.getStage());
    LogUtil.setStage(LogUtil.getStage(), "child");
    assertEquals("test:child", LogUtil.getStage());
    LogUtil.clearStage();
    assertNull(LogUtil.getStage());
  }
}
