package com.onthegomap.planetiler.util;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class MergingRangeMapTest {

  @Test
  void empty() {
    var map = MergingRangeMap.unitMap();
    assertEquals(
      List.of(
        new MergingRangeMap.Partial<>(0, 1.0, Map.of())
      ),
      map.result()
    );
  }

  @Test
  void testPartialOverlap() {
    var map = MergingRangeMap.unitMap();
    map.put(0.25, 0.75, Map.of("b", 3, "c", 4));
    map.put(0.5, 1.0, Map.of("a", 1, "b", 2));
    assertEquals(
      List.of(
        new MergingRangeMap.Partial<>(0, 0.25, Map.of()),
        new MergingRangeMap.Partial<>(0.25, 0.5, Map.of("b", 3, "c", 4)),
        new MergingRangeMap.Partial<>(0.5, 0.75, Map.of("a", 1, "b", 2, "c", 4)),
        new MergingRangeMap.Partial<>(0.75, 1.0, Map.of("a", 1, "b", 2))
      ),
      map.result()
    );
  }

  @Test
  void testPutSingle() {
    var map = MergingRangeMap.unitMap();
    map.put(0.25, 0.75, Map.of("b", 3));
    map.put(0.25, 0.75, Map.of("c", 4));
    map.put(0.5, 1.0, Map.of("a", 1));
    map.put(0.5, 1.0, Map.of("b", 2));
    assertEquals(
      List.of(
        new MergingRangeMap.Partial<>(0, 0.25, Map.of()),
        new MergingRangeMap.Partial<>(0.25, 0.5, Map.of("b", 3, "c", 4)),
        new MergingRangeMap.Partial<>(0.5, 0.75, Map.of("a", 1, "b", 2, "c", 4)),
        new MergingRangeMap.Partial<>(0.75, 1.0, Map.of("a", 1, "b", 2))
      ),
      map.result()
    );
  }

  @Test
  void testDuplicateKeys() {
    var map = MergingRangeMap.unitMap();
    map.put(0.25, 0.75, Map.of("a", 1));
    map.put(0.5, 1.0, Map.of("a", 1));
    assertEquals(
      List.of(
        new MergingRangeMap.Partial<>(0, 0.25, Map.of()),
        new MergingRangeMap.Partial<>(0.25, 1d, Map.of("a", 1))
      ),
      map.result()
    );
  }
}
