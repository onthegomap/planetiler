/*****************************************************************
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
package com.onthegomap.flatmap;

import static com.onthegomap.flatmap.TestUtils.TRANSFORM_TO_TILE;
import static com.onthegomap.flatmap.TestUtils.newGeometryCollection;
import static com.onthegomap.flatmap.TestUtils.newMultiPoint;
import static com.onthegomap.flatmap.TestUtils.newMultiPolygon;
import static com.onthegomap.flatmap.TestUtils.newPoint;
import static com.onthegomap.flatmap.TestUtils.newPolygon;
import static com.onthegomap.flatmap.geo.GeoUtils.JTS_FACTORY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.primitives.Ints;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.CoordinateXY;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryCollection;
import org.locationtech.jts.geom.LinearRing;
import org.locationtech.jts.geom.MultiPolygon;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;
import vector_tile.VectorTile;

/**
 * This class is copied from https://github.com/ElectronicChartCentre/java-vector-tile/blob/master/src/test/java/no/ecc/vectortile/VectorTileEncoderTest.java
 * and modified based on the changes in VectorTileEncoder, and adapted to junit 5.
 */
public class VectorTileEncoderTest {
  // Tests adapted from https://github.com/ElectronicChartCentre/java-vector-tile/blob/master/src/test/java/no/ecc/vectortile/VectorTileEncoderTest.java

  private static List<Integer> getCommands(Geometry geom) {
    return Ints.asList(VectorTileEncoder.encodeGeometry(TRANSFORM_TO_TILE.transform(geom)).commands());
  }

  @Test
  public void testToGeomType() {
    Geometry geometry = JTS_FACTORY.createLineString(new Coordinate[]{new CoordinateXY(1, 2), new CoordinateXY(3, 4)});
    assertEquals((byte) VectorTile.Tile.GeomType.LINESTRING.getNumber(),
      VectorTileEncoder.encodeGeometry(geometry).geomType());
  }

  @Test
  public void testCommands() {
    assertEquals(List.of(9, 6, 12, 18, 10, 12, 24, 44, 15), getCommands(newPolygon(
      3, 6,
      8, 12,
      20, 34,
      3, 6
    )));
  }

  @Test
  public void testCommandsFilter() {
    assertEquals(List.of(9, 6, 12, 18, 10, 12, 24, 44, 15), getCommands(newPolygon(
      3, 6,
      8, 12,
      8, 12,
      20, 34,
      3, 6
    )));
  }

  @Test
  public void testPoint() {
    assertEquals(List.of(9, 6, 12), getCommands(newMultiPoint(
      newPoint(3, 6)
    )));
  }

  @Test
  public void testMultiPoint() {
    assertEquals(List.of(17, 10, 14, 3, 9), getCommands(newMultiPoint(
      newPoint(5, 7),
      newPoint(3, 2)
    )));
  }

  private static VectorTileEncoder.Feature newVectorTileFeature(String layer, Geometry geom,
    Map<String, Object> attrs) {
    return new VectorTileEncoder.Feature(layer, 1, VectorTileEncoder.encodeGeometry(geom), attrs);
  }

  @Test
  public void testNullAttributeValue() {
    VectorTileEncoder vtm = new VectorTileEncoder();
    Map<String, Object> attrs = new HashMap<>();
    attrs.put("key1", "value1");
    attrs.put("key2", null);
    attrs.put("key3", "value3");

    vtm.addLayerFeatures("DEPCNT", List.of(
      newVectorTileFeature("DEPCNT", newPoint(3, 6), attrs)
    ));

    byte[] encoded = vtm.encode();
    assertNotSame(0, encoded.length);

    var decoded = VectorTileEncoder.decode(encoded);
    assertEquals(List
      .of(new VectorTileEncoder.Feature("DEPCNT", 1, VectorTileEncoder.encodeGeometry(newPoint(3, 6)), Map.of(
        "key1", "value1",
        "key3", "value3"
      ))), decoded);
    assertSameGeometries(List.of(newPoint(3, 6)), decoded);
  }

  @Test
  public void testAttributeTypes() {
    VectorTileEncoder vtm = new VectorTileEncoder();

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

    List<VectorTileEncoder.Feature> decoded = VectorTileEncoder.decode(encoded);
    assertEquals(1, decoded.size());
    Map<String, Object> decodedAttributes = decoded.get(0).attrs();
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
  public void testMultiPolygonCommands() {
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
  public void testMultiPolygon() {
    MultiPolygon mp = newMultiPolygon(
      (Polygon) newPoint(13, 16).buffer(3),
      (Polygon) newPoint(24, 25).buffer(5)
    ).reverse(); // ensure outer CCW, inner CW winding
    assertTrue(mp.isValid());

    Map<String, Object> attrs = Map.of("key1", "value1");

    VectorTileEncoder vtm = new VectorTileEncoder();
    vtm.addLayerFeatures("mp", List.of(newVectorTileFeature("mp", mp, attrs)));

    byte[] encoded = vtm.encode();
    assertTrue(encoded.length > 0);

    var features = VectorTileEncoder.decode(encoded);
    assertEquals(1, features.size());
    MultiPolygon mp2 = (MultiPolygon) features.get(0).geometry().decode();
    assertEquals(mp.getNumGeometries(), mp2.getNumGeometries());
  }

  @Test
  public void testGeometryCollectionSilentlyIgnored() {
    GeometryCollection gc = newGeometryCollection(
      newPoint(13, 16).buffer(3),
      newPoint(24, 25)
    );
    Map<String, Object> attributes = Map.of("key1", "value1");

    VectorTileEncoder vtm = new VectorTileEncoder();
    vtm.addLayerFeatures("gc", List.of(newVectorTileFeature("gc", gc, attributes)));

    byte[] encoded = vtm.encode();

    var features = VectorTileEncoder.decode(encoded);
    assertEquals(0, features.size());
  }

  // New tests added:

  @Test
  public void testRoundTripPoint() {
    testRoundTripGeometry(JTS_FACTORY.createPoint(new CoordinateXY(1, 2)));
  }

  @Test
  public void testRoundTripMultipoint() {
    testRoundTripGeometry(JTS_FACTORY.createMultiPointFromCoords(new Coordinate[]{
      new CoordinateXY(1, 2),
      new CoordinateXY(3, 4)
    }));
  }

  @Test
  public void testRoundTripLineString() {
    testRoundTripGeometry(JTS_FACTORY.createLineString(new Coordinate[]{
      new CoordinateXY(1, 2),
      new CoordinateXY(3, 4)
    }));
  }

  @Test
  public void testRoundTripPolygon() {
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
  public void testRoundTripMultiPolygon() {
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
  public void testRoundTripAttributes() {
    testRoundTripAttrs(Map.of(
      "string", "string",
      "long", 1L,
      "double", 3.5d,
      "true", true,
      "false", false
    ));
  }

  @Test
  public void testMultipleFeaturesMultipleLayer() {
    Point point = JTS_FACTORY.createPoint(new CoordinateXY(0, 0));
    Map<String, Object> attrs1 = Map.of("a", 1L, "b", 2L);
    Map<String, Object> attrs2 = Map.of("b", 3L, "c", 2L);
    byte[] encoded = new VectorTileEncoder().addLayerFeatures("layer1", List.of(
      new VectorTileEncoder.Feature("layer1", 1L, VectorTileEncoder.encodeGeometry(point), attrs1),
      new VectorTileEncoder.Feature("layer1", 2L, VectorTileEncoder.encodeGeometry(point), attrs2)
    )).addLayerFeatures("layer2", List.of(
      new VectorTileEncoder.Feature("layer2", 3L, VectorTileEncoder.encodeGeometry(point), attrs1)
    )).encode();

    List<VectorTileEncoder.Feature> decoded = VectorTileEncoder.decode(encoded);
    assertEquals(attrs1, decoded.get(0).attrs());
    assertEquals("layer1", decoded.get(0).layer());

    assertEquals(attrs2, decoded.get(1).attrs());
    assertEquals("layer1", decoded.get(1).layer());

    assertEquals(attrs1, decoded.get(2).attrs());
    assertEquals("layer2", decoded.get(2).layer());
  }

  private void testRoundTripAttrs(Map<String, Object> attrs) {
    testRoundTrip(JTS_FACTORY.createPoint(new CoordinateXY(0, 0)), "layer", attrs, 1);
  }

  private void testRoundTripGeometry(Geometry input) {
    testRoundTrip(input, "layer", Map.of(), 1);
  }

  private void testRoundTrip(Geometry input, String layer, Map<String, Object> attrs, long id) {
    VectorTileEncoder.VectorGeometry encodedGeom = VectorTileEncoder.encodeGeometry(input);
    Geometry output = encodedGeom.decode();
    assertTrue(input.equalsExact(output), "\n" + input + "\n!=\n" + output);

    byte[] encoded = new VectorTileEncoder().addLayerFeatures(layer, List.of(
      new VectorTileEncoder.Feature(layer, id, VectorTileEncoder.encodeGeometry(input), attrs)
    )).encode();

    List<VectorTileEncoder.Feature> decoded = VectorTileEncoder.decode(encoded);
    VectorTileEncoder.Feature expected = new VectorTileEncoder.Feature(layer, id,
      VectorTileEncoder.encodeGeometry(input), attrs);
    assertEquals(List.of(expected), decoded);
    assertSameGeometries(List.of(input), decoded);
  }

  private void assertSameGeometries(List<Geometry> expected, List<VectorTileEncoder.Feature> actual) {
    assertEquals(expected, actual.stream().map(d -> d.geometry().decode()).toList());
  }
}
