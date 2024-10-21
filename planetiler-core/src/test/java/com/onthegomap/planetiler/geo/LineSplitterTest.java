package com.onthegomap.planetiler.geo;

import static com.onthegomap.planetiler.TestUtils.newLineString;
import static com.onthegomap.planetiler.TestUtils.newPoint;
import static com.onthegomap.planetiler.TestUtils.round;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

class LineSplitterTest {
  @ParameterizedTest
  @CsvSource({
    "0,1",
    "0,0.25",
    "0.75, 1",
    "0.25, 0.75",
  })
  void testSingleSegment(double start, double end) {
    var l = new LineSplitter(newLineString(0, 0, 2, 1));
    assertEquals(
      newLineString(start * 2, start, end * 2, end),
      l.get(start, end)
    );
  }

  @Test
  void testLength2() {
    var l = new LineSplitter(newLineString(0, 0, 1, 2, 2, 4));
    assertEquals(
      newLineString(0, 0, 0.5, 1),
      l.get(0, 0.25)
    );
    assertEquals(
      newLineString(0.2, 0.4, 0.5, 1),
      l.get(0.1, 0.25)
    );
    assertEquals(
      newLineString(0.5, 1, 1, 2),
      l.get(0.25, 0.5)
    );
    assertEquals(
      newLineString(0.5, 1, 1, 2, 1.5, 3),
      l.get(0.25, 0.75)
    );
    assertEquals(
      newLineString(1, 2, 1.5, 3),
      l.get(0.5, 0.75)
    );
    assertEquals(
      newLineString(1.2, 2.4, 1.5, 3),
      l.get(0.6, 0.75)
    );
    assertEquals(
      newLineString(1.5, 3, 2, 4),
      l.get(0.75, 1)
    );
  }

  @ParameterizedTest
  @ValueSource(doubles = {
    0, 0.00001, 0.1, 0.49999, 0.5, 0.50001, 0.9, 0.99999, 1.0
  })
  void testDistanceAlongLine(double ratio) {
    var l = new LineSplitter(newLineString(0, 0, 1, 0.5, 2, 1));
    assertEquals(
      round(newPoint(ratio * 2, ratio)),
      round(l.get(ratio))
    );
  }

  @Test
  void testInvalid() {
    var l = new LineSplitter(newLineString(0, 0, 1, 2, 2, 4));
    assertThrows(IllegalArgumentException.class, () -> l.get(-0.1, 0.5));
    assertThrows(IllegalArgumentException.class, () -> l.get(0.9, 1.1));
    assertThrows(IllegalArgumentException.class, () -> l.get(0.6, 0.5));
  }
}
