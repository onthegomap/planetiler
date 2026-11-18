package com.onthegomap.planetiler.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

class LogUtilTest {
  @Test
  void testStageHandling() {
    LogUtil.clearStage();
    assertNull(LogUtil.getStage());
    LogUtil.setStage("test");
    assertEquals("test", LogUtil.getStage());
    LogUtil.setStage(LogUtil.getStage(), "child");
    assertEquals("test:child", LogUtil.getStage());
    LogUtil.clearStage();
    assertNull(LogUtil.getStage());
  }

  @Test
  void testRunInStageGoesBackToPreviousStage() {
    LogUtil.setStage("outer");
    LogUtil.runInStage("inner", () -> assertEquals("inner", LogUtil.getStage()));
    assertEquals("outer", LogUtil.getStage());

    LogUtil.clearStage();
    assertThrows(IllegalStateException.class, () -> LogUtil.runInStage("inner", () -> {
      throw new IllegalStateException();
    }));
    assertNull(LogUtil.getStage());
  }
}
