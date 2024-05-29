package com.onthegomap.planetiler.util;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class RangeMapMapTest {

  @Test
  void empty() {
    var map = new RangeMapMap();
    assertEquals(
      List.of(
        new RangeMapMap.Partial(0, 1.0, Map.of())
      ),
      map.result()
    );
  }

  @Test
  void testPartialOverlap() {
    var map = new RangeMapMap();
    map.put(0.25, 0.75, Map.of("b", 3, "c", 4));
    map.put(0.5, 1.0, Map.of("a", 1, "b", 2));
    assertEquals(
      List.of(
        new RangeMapMap.Partial(0, 0.25, Map.of()),
        new RangeMapMap.Partial(0.25, 0.5, Map.of("b", 3, "c", 4)),
        new RangeMapMap.Partial(0.5, 0.75, Map.of("a", 1, "b", 2, "c", 4)),
        new RangeMapMap.Partial(0.75, 1.0, Map.of("a", 1, "b", 2))
      ),
      map.result()
    );
  }

  @Test
  void testPutSingle() {
    var map = new RangeMapMap();
    map.put(0.25, 0.75, "b", 3);
    map.put(0.25, 0.75, "c", 4);
    map.put(0.5, 1.0, "a", 1);
    map.put(0.5, 1.0, "b", 2);
    assertEquals(
      List.of(
        new RangeMapMap.Partial(0, 0.25, Map.of()),
        new RangeMapMap.Partial(0.25, 0.5, Map.of("b", 3, "c", 4)),
        new RangeMapMap.Partial(0.5, 0.75, Map.of("a", 1, "b", 2, "c", 4)),
        new RangeMapMap.Partial(0.75, 1.0, Map.of("a", 1, "b", 2))
      ),
      map.result()
    );
  }

  @Test
  void testDuplicateKeys() {
    var map = new RangeMapMap();
    map.put(0.25, 0.75, Map.of("a", 1));
    map.put(0.5, 1.0, Map.of("a", 1));
    assertEquals(
      List.of(
        new RangeMapMap.Partial(0, 0.25, Map.of()),
        new RangeMapMap.Partial(0.25, 1d, Map.of("a", 1))
      ),
      map.result()
    );
  }
}
