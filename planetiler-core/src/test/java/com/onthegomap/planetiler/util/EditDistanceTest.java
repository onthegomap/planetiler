package com.onthegomap.planetiler.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class EditDistanceTest {

  @ParameterizedTest
  @CsvSource({
    "'', '', 0",
    "abc, abc, 0",
    "abc, abd, 1",
    "abc, ab, 1",
    "maxzom, maxzoom, 1",
    "kitten, sitting, 3",
  })
  void testEditDistance(String a, String b, int expected) {
    assertEquals(expected, EditDistance.editDistance(a, b));
    assertEquals(expected, EditDistance.editDistance(b, a));
  }

  @Test
  void testClosest() {
    var candidates = List.of("maxzoom", "minzoom", "output", "shortbread", "openmaptiles");
    assertEquals("maxzoom", EditDistance.closest("maxzom", candidates));
    assertEquals("shortbread", EditDistance.closest("shortbred", candidates));
    assertEquals("openmaptiles", EditDistance.closest("openmaptile", candidates));
    assertNull(EditDistance.closest("area", candidates));
    assertNull(EditDistance.closest("xyz", candidates));
  }
}
