/* ****************************************************************
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 ****************************************************************/
package com.onthegomap.planetiler;

import static com.onthegomap.planetiler.TestUtils.*;
import static com.onthegomap.planetiler.VectorTile.zigZagEncode;
import static com.onthegomap.planetiler.geo.GeoUtils.JTS_FACTORY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.DynamicTest.dynamicTest;

import com.google.common.primitives.Ints;
import com.onthegomap.planetiler.geo.GeoUtils;
import com.onthegomap.planetiler.geo.GeometryException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.CoordinateXY;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryCollection;
import org.locationtech.jts.geom.LinearRing;
import org.locationtech.jts.geom.MultiPolygon;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.geom.PrecisionModel;
import org.locationtech.jts.geom.util.AffineTransformation;
import org.locationtech.jts.geom.util.NoninvertibleTransformationException;
import org.locationtech.jts.precision.GeometryPrecisionReducer;
import vector_tile.VectorTileProto;

/**
 * This class is copied from
 * https://github.com/ElectronicChartCentre/java-vector-tile/blob/master/src/test/java/no/ecc/vectortile/VectorTileEncoderTest.java
 * and modified based on the changes in VectorTileEncoder, and adapted to junit 5.
 */
class VectorTileTest {
  // Tests adapted from https://github.com/ElectronicChartCentre/java-vector-tile/blob/master/src/test/java/no/ecc/vectortile/VectorTileEncoderTest.java

  private static List<Integer> getCommands(Geometry geom) {
    return Ints.asList(VectorTile.encodeGeometry(TRANSFORM_TO_TILE.transform(geom)).commands());
  }

  @Test
  void testToGeomType() {
    Geometry geometry = JTS_FACTORY.createLineString(new Coordinate[]{new CoordinateXY(1, 2), new CoordinateXY(3, 4)});
    assertEquals((byte) VectorTileProto.Tile.GeomType.LINESTRING.getNumber(),
      VectorTile.encodeGeometry(geometry).geomType().asByte());
  }

  @Test
  void testCommands() {
    assertEquals(List.of(9, 6, 12, 18, 10, 12, 24, 44, 15), getCommands(newPolygon(
      3, 6,
      8, 12,
      20, 34,
      3, 6
    )));
  }

  @Test
  void testCommandsFilter() {
    assertEquals(List.of(9, 6, 12, 18, 10, 12, 24, 44, 15), getCommands(newPolygon(
      3, 6,
      8, 12,
      8, 12,
      20, 34,
      3, 6
    )));
  }

  @Test
  void testPoint() {
    assertEquals(List.of(9, 6, 12), getCommands(newMultiPoint(
      newPoint(3, 6)
    )));
  }

  @Test
  void testMultiPoint() {
    assertEquals(List.of(17, 10, 14, 3, 9), getCommands(newMultiPoint(
      newPoint(5, 7),
      newPoint(3, 2)
    )));
  }

  private static VectorTile.Feature newVectorTileFeature(String layer, Geometry geom,
    Map<String, Object> attrs) {
    return new VectorTile.Feature(layer, 1, VectorTile.encodeGeometry(geom), attrs);
  }

  @Test
  void testNullAttributeValue() {
    VectorTile vtm = new VectorTile();
    Map<String, Object> attrs = new HashMap<>();
    attrs.put("key1", "value1");
    attrs.put("key2", null);
    attrs.put("key3", "value3");

    vtm.addLayerFeatures("DEPCNT", List.of(
      newVectorTileFeature("DEPCNT", newPoint(3, 6), attrs)
    ));

    byte[] encoded = vtm.encode();
    assertNotSame(0, encoded.length);

    var decoded = VectorTile.decode(encoded);
    assertEquals(List
      .of(new VectorTile.Feature("DEPCNT", 1, VectorTile.encodeGeometry(newPoint(3, 6)), Map.of(
        "key1", "value1",
        "key3", "value3"
      ))), decoded);
    assertSameGeometries(List.of(newPoint(3, 6)), decoded);
  }

  @Test
  void testAttributeTypes() {
    VectorTile vtm = new VectorTile();

    Map<String, Object> attrs = Map.of(
      "key1", "value1",
      "key2", 123,
      "key3", 234.1f,
      "key4", 567.123d,
      "key5", (long) -123,
      "key6", "value6",
      "key7", Boolean.TRUE,
      "key8", Boolean.FALSE
    );

    vtm.addLayerFeatures("DEPCNT", List.of(newVectorTileFeature("DEPCNT", newPoint(3, 6), attrs)));

    byte[] encoded = vtm.encode();
    assertNotSame(0, encoded.length);

    List<VectorTile.Feature> decoded = VectorTile.decode(encoded);
    assertEquals(1, decoded.size());
    Map<String, Object> decodedAttributes = decoded.getFirst().attrs();
    assertEquals("value1", decodedAttributes.get("key1"));
    assertEquals(123L, decodedAttributes.get("key2"));
    assertEquals(234.1f, decodedAttributes.get("key3"));
    assertEquals(567.123d, decodedAttributes.get("key4"));
    assertEquals((long) -123, decodedAttributes.get("key5"));
    assertEquals("value6", decodedAttributes.get("key6"));
    assertEquals(Boolean.TRUE, decodedAttributes.get("key7"));
    assertEquals(Boolean.FALSE, decodedAttributes.get("key8"));
  }

  @Test
  void testMultiPolygonCommands() {
    // see https://github.com/mapbox/vector-tile-spec/blob/master/2.1/README.md
    assertEquals(List.of(
      9, 0, 0, 26, 20, 0, 0, 20, 19, 0, 15,
      9, 22, 2, 26, 18, 0, 0, 18, 17, 0, 15,
      9, 4, 13, 26, 0, 8, 8, 0, 0, 7, 15
    ), getCommands(newMultiPolygon(
      newPolygon(0, 0,
        10, 0,
        10, 10,
        0, 10,
        0, 0
      ),
      newPolygon(
        11, 11,
        20, 11,
        20, 20,
        11, 20,
        11, 11
      ),
      newPolygon(
        13, 13,
        13, 17,
        17, 17,
        17, 13,
        13, 13
      )
    )));
  }

  @Test
  void testMultiPolygon() {
    MultiPolygon mp = newMultiPolygon(
      (Polygon) newPoint(13, 16).buffer(3),
      (Polygon) newPoint(24, 25).buffer(5)
    ).reverse(); // ensure outer CCW, inner CW winding
    assertTrue(mp.isValid());

    Map<String, Object> attrs = Map.of("key1", "value1");

    VectorTile vtm = new VectorTile();
    vtm.addLayerFeatures("mp", List.of(newVectorTileFeature("mp", mp, attrs)));

    byte[] encoded = vtm.encode();
    assertTrue(encoded.length > 0);

    var features = VectorTile.decode(encoded);
    assertEquals(1, features.size());
    MultiPolygon mp2 = (MultiPolygon) decodeSilently(features.getFirst().geometry());
    assertEquals(mp.getNumGeometries(), mp2.getNumGeometries());
  }

  @Test
  void testGeometryCollectionSilentlyIgnored() {
    GeometryCollection gc = newGeometryCollection(
      newPoint(13, 16).buffer(3),
      newPoint(24, 25)
    );
    Map<String, Object> attributes = Map.of("key1", "value1");

    VectorTile vtm = new VectorTile();
    vtm.addLayerFeatures("gc", List.of(newVectorTileFeature("gc", gc, attributes)));

    byte[] encoded = vtm.encode();

    var features = VectorTile.decode(encoded);
    assertEquals(0, features.size());
  }

  // New tests added:

  @Test
  void testRoundTripPoint() {
    testRoundTripGeometry(JTS_FACTORY.createPoint(new CoordinateXY(1, 2)));
  }

  @Test
  void testRoundTripMultipoint() {
    testRoundTripGeometry(JTS_FACTORY.createMultiPointFromCoords(new Coordinate[]{
      new CoordinateXY(1, 2),
      new CoordinateXY(3, 4)
    }));
  }

  @Test
  void testRoundTripLineString() {
    testRoundTripGeometry(JTS_FACTORY.createLineString(new Coordinate[]{
      new CoordinateXY(1, 2),
      new CoordinateXY(3, 4)
    }));
  }

  @Test
  void testRoundTripPolygon() {
    testRoundTripGeometry(JTS_FACTORY.createPolygon(
      JTS_FACTORY.createLinearRing(new Coordinate[]{
        new CoordinateXY(0, 0),
        new CoordinateXY(4, 0),
        new CoordinateXY(4, 4),
        new CoordinateXY(0, 4),
        new CoordinateXY(0, 0)
      }),
      new LinearRing[]{
        JTS_FACTORY.createLinearRing(new Coordinate[]{
          new CoordinateXY(1, 1),
          new CoordinateXY(1, 2),
          new CoordinateXY(2, 2),
          new CoordinateXY(2, 1),
          new CoordinateXY(1, 1)
        })
      }
    ));
  }

  @Test
  void testRoundTripMultiPolygon() {
    testRoundTripGeometry(JTS_FACTORY.createMultiPolygon(new Polygon[]{
      JTS_FACTORY.createPolygon(new Coordinate[]{
        new CoordinateXY(0, 0),
        new CoordinateXY(1, 0),
        new CoordinateXY(1, 1),
        new CoordinateXY(0, 1),
        new CoordinateXY(0, 0)
      }),
      JTS_FACTORY.createPolygon(new Coordinate[]{
        new CoordinateXY(3, 0),
        new CoordinateXY(4, 0),
        new CoordinateXY(4, 1),
        new CoordinateXY(3, 1),
        new CoordinateXY(3, 0)
      })
    }));
  }

  @Test
  void testRoundTripAttributes() {
    testRoundTripAttrs(Map.of(
      "string", "string",
      "long", 1L,
      "double", 3.5d,
      "true", true,
      "false", false
    ));
  }

  @Test
  void testMultipleFeaturesMultipleLayer() {
    Point point = JTS_FACTORY.createPoint(new CoordinateXY(0, 0));
    Map<String, Object> attrs1 = Map.of("a", 1L, "b", 2L);
    Map<String, Object> attrs2 = Map.of("b", 3L, "c", 2L);
    byte[] encoded = new VectorTile().addLayerFeatures("layer1", List.of(
      new VectorTile.Feature("layer1", 1L, VectorTile.encodeGeometry(point), attrs1),
      new VectorTile.Feature("layer1", 2L, VectorTile.encodeGeometry(point), attrs2)
    )).addLayerFeatures("layer2", List.of(
      new VectorTile.Feature("layer2", 3L, VectorTile.encodeGeometry(point), attrs1)
    )).encode();

    List<VectorTile.Feature> decoded = VectorTile.decode(encoded);
    assertEquals(attrs1, decoded.get(0).attrs());
    assertEquals("layer1", decoded.get(0).layer());

    assertEquals(attrs2, decoded.get(1).attrs());
    assertEquals("layer1", decoded.get(1).layer());

    assertEquals(attrs1, decoded.get(2).attrs());
    assertEquals("layer2", decoded.get(2).layer());
  }

  @ParameterizedTest
  @CsvSource({
    "true,true,-1,-1,257,257",
    "true,true,-5,-5,260,260",
    "false,false,-1,-1,254,254",
    "true,false,0,-1,257,257",
    "true,false,-1,0,257,257",
    "true,false,-1,-1,256,257",
    "true,false,-1,-1,257,256",

    "false,false,0,0,1,1",
    "false,false,1,1,2,2",
    "false,false,1,1,2,2",

    "false,false,-10,-10,-5,-5",
    "false,false,260,-10,270,5",
    "false,false,-10,260,-5,270",
    "false,false,260,260,270,270",

    "true,false,1,-1,257,257",
    "true,false,1,-1,255,257",
    "false,false,1,-1,255,255",
  })
  void testRectangleIsFillOrEdge(boolean isFillOrEdge, boolean isFill, double x1, double y1, double x2, double y2) {
    assertIsFillOrEdge(isFillOrEdge, isFill, rectangle(x1, y1, x2, y2));
  }

  @Test
  void testRectangleWithSlantedEdgeIsNotFill() {
    assertIsFillOrEdge(false, false, newPolygon(
      1, -1,
      257, -1,
      257, 257,
      2, 257,
      1, -1
    ));
  }

  @ParameterizedTest
  @CsvSource({
    "true,1,-1,1,257",
    "false,1,1,1,257",
    "false,1,-1,1,255",
    "false,1,-1,2,257"
  })
  void testLineIsEdge(boolean isFillOrEdge, double x1, double y1, double x2, double y2) {
    assertIsFillOrEdge(isFillOrEdge, false, newLineString(
      x1, y1,
      x2, y2
    ));
  }

  @Test
  void testCrossBoundaryNotFillOrEdge() {
    assertIsFillOrEdge(false, false, newPolygon(
      -1, -1,
      257, -1,
      -1, 257,
      -1, -1
    ));
    assertIsFillOrEdge(false, false, newPolygon(
      257, -1,
      -1, 257,
      -1, -1,
      257, -1
    ));
    assertIsFillOrEdge(false, false, newPolygon(
      -1, 257,
      -1, -1,
      257, -1,
      -1, 257
    ));
    assertIsFillOrEdge(false, false, newPolygon(
      -1, -1,
      513, -1,
      -1, 513,
      -1, -1
    ));
  }

  private static void assertIsFillOrEdge(boolean isFillOrEdge, boolean isFill, Geometry geom) {
    for (int rotation : List.of(0, 90, 180, 270)) {
      var rectangle =
        AffineTransformation.rotationInstance(Math.PI * rotation / 180, 128, 128).transform(geom);
      for (int scale = 0; scale < 4; scale++) {
        assertEquals(isFillOrEdge, VectorTile.encodeGeometry(rectangle, scale).isFillOrEdge(),
          "scale=" + scale + " rotation=" + rotation);
        assertEquals(isFill, VectorTile.encodeGeometry(rectangle, scale).isFill(),
          "scale=" + scale + " rotation=" + rotation);
      }
    }
  }

  private void testRoundTripAttrs(Map<String, Object> attrs) {
    testRoundTrip(JTS_FACTORY.createPoint(new CoordinateXY(0, 0)), "layer", attrs, 1);
  }

  private void testRoundTripGeometry(Geometry input) {
    testRoundTrip(input, "layer", Map.of(), 1);
  }

  private void testRoundTrip(Geometry input, String layer, Map<String, Object> attrs, long id) {
    VectorTile.VectorGeometry encodedGeom = VectorTile.encodeGeometry(input);
    Geometry output = decodeSilently(encodedGeom);
    assertTrue(input.equalsExact(output), "%n%s%n!=%n%s".formatted(input, output));

    byte[] encoded = new VectorTile().addLayerFeatures(layer, List.of(
      new VectorTile.Feature(layer, id, VectorTile.encodeGeometry(input), attrs)
    )).encode();

    List<VectorTile.Feature> decoded = VectorTile.decode(encoded);
    VectorTile.Feature expected = new VectorTile.Feature(layer, id,
      VectorTile.encodeGeometry(input), attrs);
    assertEquals(List.of(expected), decoded);
    assertSameGeometries(List.of(input), decoded);
  }

  private void assertSameGeometries(List<Geometry> expected, List<VectorTile.Feature> actual) {
    assertEquals(expected, actual.stream().map(d -> decodeSilently(d.geometry())).toList());
  }

  @TestFactory
  Stream<DynamicTest> testScaleUnscale() throws NoninvertibleTransformationException {
    var scales = List.of(0, 1, 2, 16);
    var scaleUp = AffineTransformation.scaleInstance(256d / 4096, 256d / 4096);
    var scaleDown = scaleUp.getInverse();
    return Stream.of(
      newPoint(0, 0),
      newPoint(0.25, -0.25),
      newPoint(1.25, 1.25),
      newPoint(1.5, 1.5),
      newMultiPoint(
        newPoint(1.25, 1.25),
        newPoint(1.5, 1.5)
      ),
      newLineString(0, 0, 1.2, 1.2),
      newLineString(0, 0, 0.1, 0.1),
      newLineString(0, 0, 1, 1, 1.2, 1.2, 2, 2),
      newLineString(8000, 8000, 8000, 8001, 8001, 8001),
      newLineString(-4000, -4000, -4000, -4001, -4001, -4001),
      newMultiLineString(
        newLineString(0, 0, 1, 1),
        newLineString(1.1, 1.1, 2, 2)
      ),
      newMultiLineString(
        newLineString(0, 0, 0.1, 0.1),
        newLineString(1.1, 1.1, 2, 2)
      ),
      newMultiLineString(
        newLineString(-10, -10, -9, -9),
        newLineString(0, 0, 0.1, 0.1),
        newLineString(1.1, 1.1, 2, 2)
      ),
      newPolygon(0, 0, 1, 0, 1, 1, 0, 1, 0, 0),
      newPolygon(0, 0, 0.1, 0, 0.1, 0.1, 0, 0.1, 0, 0),
      newPolygon(0, 0, 1, 0, 1, 0.1, 1, 1, 0, 1, 0, 0),
      newMultiPolygon(
        newPolygon(0, 0, 1, 0, 1, 1, 0, 1, 0, 0),
        newPolygon(0, 0, -1, 0, -1, -1, 0, -1, 0, 0)
      ),
      newPolygon(0, 0, 1, 0, 1, 1, 0, 1, 0, 0.1, 0, 0)
    ).map(scaleUp::transform)
      .flatMap(geometry -> scales.stream().flatMap(scale -> Stream.of(
        dynamicTest(scaleDown.transform(geometry) + " scale: " + scale, () -> {
          PrecisionModel pm = new PrecisionModel((4096 << scale) / 256d);
          assertSameGeometry(
            GeometryPrecisionReducer.reduce(geometry, pm),
            VectorTile.encodeGeometry(geometry, scale).decode()
          );
        }),
        dynamicTest(scaleDown.transform(geometry) + " unscale: " + scale, () -> {
          PrecisionModel pm = new PrecisionModel((4096 << scale) / 256d);
          PrecisionModel pm0 = new PrecisionModel(4096d / 256);
          assertSameGeometry(
            GeometryPrecisionReducer.reduce(GeometryPrecisionReducer.reduce(geometry, pm), pm0),
            VectorTile.encodeGeometry(geometry, scale).unscale().decode()
          );
        })
      )
      ));
  }

  @Test
  void testUnscaleDegenerate() throws GeometryException {
    var lessThanOnePx = 256d / 4096 / 4;
    var encoded = VectorTile.encodeGeometry(newLineString(0, 0, lessThanOnePx, lessThanOnePx), 2);
    assertEquals(6, encoded.commands().length);
    var unscaled = encoded.unscale();
    assertEquals(0, unscaled.commands().length);
    assertFalse(encoded.isEmpty());
    assertTrue(unscaled.isEmpty());
    assertEquals(GeoUtils.EMPTY_GEOMETRY, unscaled.decode());
    var reEncoded = VectorTile.encodeGeometry(unscaled.decode());
    assertEquals(0, reEncoded.commands().length);
  }

  @Test
  void testFilterPointsOutsideBuffer() {
    assertArrayEquals(
      new int[0],
      VectorTile.encodeGeometry(newPoint(-5, -5))
        .filterPointsOutsideBuffer(4).commands()
    );
    assertArrayEquals(
      new int[]{
        VectorTile.Command.MOVE_TO.value | (1 << 3),
        zigZagEncode((int) (-5d * 4096 / 256)),
        zigZagEncode((int) (-5d * 4096 / 256)),
      },
      VectorTile.encodeGeometry(newPoint(-5, -5))
        .filterPointsOutsideBuffer(5).commands()
    );
  }

  @Test
  void testFilterMultiPointsAllOutsideBuffer() {
    assertArrayEquals(
      new int[0],
      VectorTile.encodeGeometry(newMultiPoint(
        newPoint(-5, -5),
        newPoint(261, 261)
      )).filterPointsOutsideBuffer(4).commands()
    );
  }

  @Test
  void testFilterMultiPointsFirstOutsideBuffer() {
    assertArrayEquals(
      new int[]{
        VectorTile.Command.MOVE_TO.value | (1 << 3),
        zigZagEncode(4096),
        zigZagEncode(4096),
      },
      VectorTile.encodeGeometry(newMultiPoint(
        newPoint(-5, -5),
        newPoint(256, 256)
      )).filterPointsOutsideBuffer(4).commands()
    );
  }

  @Test
  void testFilterMultiPointsLastOutsideBuffer() {
    assertArrayEquals(
      new int[]{
        VectorTile.Command.MOVE_TO.value | (1 << 3),
        zigZagEncode(4096),
        zigZagEncode(4096),
      },
      VectorTile.encodeGeometry(newMultiPoint(
        newPoint(256, 256),
        newPoint(-5, -5)
      )).filterPointsOutsideBuffer(4).commands()
    );
  }

  @TestFactory
  Stream<DynamicTest> testCountInternalGeometries() {
    record Case(int expected, Geometry geom) {}
    return Stream.of(
      new Case(1, newPoint(0, 0)),
      new Case(2, newMultiPoint(newPoint(0, 0), newPoint(0, 1))),
      new Case(3, newMultiPoint(newPoint(0, 0), newPoint(0, 1), newPoint(0, 2))),
      new Case(1, newLineString(0, 0, 1, 1)),
      new Case(2, newMultiLineString(
        newLineString(0, 0, 1, 1),
        newLineString(0, 0, 2, 2)
      )),
      new Case(3, newMultiLineString(
        newLineString(0, 0, 1, 1),
        newLineString(0, 0, 2, 2),
        newLineString(0, 0, 2, 3)
      )),
      new Case(1, rectangle(0, 1)),
      new Case(2, newMultiPolygon(
        rectangle(0, 1),
        rectangle(3, 4)
      )),
      new Case(3, newMultiPolygon(
        rectangle(0, 1),
        rectangle(3, 4),
        rectangle(6, 8)
      ))
    ).map(test -> dynamicTest(test.toString(),
      () -> {
        var feature = new VectorTile.Feature(
          "layer", 1, VectorTile.encodeGeometry(test.geom), Map.of()
        );
        var tile = new VectorTile()
          .addLayerFeatures("layer", List.of(feature));
        assertEquals(test.expected, VectorTile.countGeometries(tile.toProto().getLayers(0).getFeatures(0)));
      }));
  }

  private static void assertArrayEquals(int[] a, int[] b) {
    assertEquals(
      IntStream.of(a).boxed().toList(),
      IntStream.of(b).boxed().toList()
    );
  }

  private void assertSameGeometry(Geometry expected, Geometry actual) {
    if (expected.isEmpty() && actual.isEmpty()) {
      // OK
    } else {
      assertSameNormalizedFeature(expected, actual);
    }
  }
}
