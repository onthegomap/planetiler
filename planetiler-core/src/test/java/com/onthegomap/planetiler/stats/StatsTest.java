package com.onthegomap.planetiler.stats;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

import com.onthegomap.planetiler.util.LogUtil;
import java.util.Map;
import org.junit.jupiter.api.Test;

class StatsTest {
  @Test
  void testDefaultStats() {
    var a = Stats.inMemory();
    var b = DefaultStats.get();
    assertSame(a, b);
  }

  @Test
  void captureDataErrors() {
    var a = Stats.inMemory();
    assertEquals(Map.of(), a.dataErrors());
    a.dataError("a");
    a.dataError("a");
    a.dataError("b");
    assertEquals(Map.of("a", 2L, "b", 1L), a.dataErrors());
  }

  @Test
  void stageLogPrefixEndsWithStage() {
    var stats = Stats.inMemory();
    // the log prefix is per thread, and other tests on this thread may have left one behind
    String before = LogUtil.getStage();
    var outer = stats.startStage("outer");
    var inner = stats.startStage("inner");
    assertEquals("inner", LogUtil.getStage());
    inner.stop();
    assertEquals("outer", LogUtil.getStage());
    outer.stop();
    assertEquals(before, LogUtil.getStage());
  }
}
