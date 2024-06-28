package com.onthegomap.planetiler.reader.parquet;

import static org.junit.jupiter.api.AssertionFailureBuilder.assertionFailure;
import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.CoordinateSequence;
import org.locationtech.jts.geom.CoordinateSequences;
import org.locationtech.jts.geom.CoordinateXY;
import org.locationtech.jts.geom.CoordinateXYZM;
import org.locationtech.jts.geom.impl.CoordinateArraySequence;

class CoordinateSequenceBuilderTest {
  @ParameterizedTest
  @CsvSource({
    "2, 2, 0",
    "3, 3, 0",
    "4, 4, 1",
  })
  void testEmpty(int components, int dims, int measures) {
    CoordinateSequence cs = new CoordinateSequenceBuilder(components);
    assertCoordinateSequence(new CoordinateArraySequence(new Coordinate[]{}, dims, measures), cs);
  }

  @Test
  void testSingleXY() {
    CoordinateSequence cs = new CoordinateSequenceBuilder(2);
    cs.setOrdinate(0, 0, 1);
    cs.setOrdinate(0, 1, 2);
    assertCoordinateSequence(new CoordinateArraySequence(new Coordinate[]{
      new CoordinateXY(1, 2)
    }), cs);
  }

  @Test
  void testDoubleXYZ() {
    CoordinateSequence cs = new CoordinateSequenceBuilder(3);
    cs.setOrdinate(0, 0, 1);
    cs.setOrdinate(0, 1, 2);

    cs.setOrdinate(1, 0, 1);
    cs.setOrdinate(1, 1, 2);
    cs.setOrdinate(1, 2, 3);
    assertCoordinateSequence(new CoordinateArraySequence(new Coordinate[]{
      new Coordinate(1, 2, 0),
      new Coordinate(1, 2, 3)
    }), cs);
  }

  @Test
  void testTripleXYZM() {
    CoordinateSequence cs = new CoordinateSequenceBuilder(4);
    cs.setOrdinate(0, 0, 1);
    cs.setOrdinate(0, 1, 2);

    cs.setOrdinate(1, 0, 1);
    cs.setOrdinate(1, 1, 2);
    cs.setOrdinate(1, 2, 3);

    cs.setOrdinate(2, 0, 1);
    cs.setOrdinate(2, 1, 2);
    cs.setOrdinate(2, 2, 3);
    cs.setOrdinate(2, 3, 4);

    assertCoordinateSequence(new CoordinateArraySequence(new Coordinate[]{
      new CoordinateXYZM(1, 2, 0, 0),
      new CoordinateXYZM(1, 2, 3, 0),
      new CoordinateXYZM(1, 2, 3, 4),
    }), cs);
  }

  private static void assertCoordinateSequence(CoordinateSequence expected, CoordinateSequence actual) {
    assertEquals(expected.getDimension(), actual.getDimension(), "dimension");
    assertEquals(expected.getMeasures(), actual.getMeasures(), "measures");
    if (!CoordinateSequences.isEqual(expected, actual)) {
      assertionFailure()
        .expected(CoordinateSequences.toString(expected))
        .actual(CoordinateSequences.toString(actual))
        .buildAndThrow();
    }
  }
}
