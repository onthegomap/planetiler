package com.onthegomap.planetiler.geo;

import static com.onthegomap.planetiler.TestUtils.assertSameNormalizedFeature;
import static com.onthegomap.planetiler.TestUtils.newLineString;
import static com.onthegomap.planetiler.TestUtils.newPolygon;
import static com.onthegomap.planetiler.TestUtils.rectangle;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import org.junit.jupiter.api.Test;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Polygonal;
import org.locationtech.jts.geom.util.AffineTransformation;

class DouglasPeuckerSimplifierTest {

  final int[] rotations = new int[]{0, 45, 90, 180, 270};

  private void testSimplify(Geometry in, Geometry expected, double amount) {
    for (int rotation : rotations) {
      var rotate = AffineTransformation.rotationInstance(Math.PI * rotation / 180);
      var expRot = rotate.transform(expected);
      var inRot = rotate.transform(in);
      assertSameNormalizedFeature(
        expRot,
        DouglasPeuckerSimplifier.simplify(inRot, amount)
      );

      // ensure the List<Coordinate> version also works...
      List<Coordinate> inList = List.of(inRot.getCoordinates());
      List<Coordinate> expList = List.of(expRot.getCoordinates());
      List<Coordinate> actual = DouglasPeuckerSimplifier.simplify(inList, amount, in instanceof Polygonal);
      assertEquals(expList, actual);
    }
  }

  @Test
  void testSimplify2Points() {
    testSimplify(newLineString(
      0, 0,
      10, 10
    ), newLineString(
      0, 0,
      10, 10
    ), 1);
  }

  @Test
  void testRemoveAPoint() {
    testSimplify(newLineString(
      0, 0,
      5, 0.9,
      10, 0
    ), newLineString(
      0, 0,
      10, 0
    ), 1);
  }

  @Test
  void testKeepAPoint() {
    testSimplify(newLineString(
      0, 0,
      5, 1.1,
      10, 0
    ), newLineString(
      0, 0,
      5, 1.1,
      10, 0
    ), 1);
  }

  @Test
  void testPolygonLeaveAPoint() {
    testSimplify(
      rectangle(0, 10),
      newPolygon(
        0, 0,
        10, 0,
        10, 10,
        0, 0
      ),
      20
    );
  }
}
