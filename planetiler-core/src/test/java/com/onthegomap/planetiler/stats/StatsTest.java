package com.onthegomap.planetiler.stats;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

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
}
