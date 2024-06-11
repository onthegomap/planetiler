package com.onthegomap.planetiler;

import static com.onthegomap.planetiler.TestUtils.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.onthegomap.planetiler.config.PlanetilerConfig;
import com.onthegomap.planetiler.geo.GeoUtils;
import com.onthegomap.planetiler.geo.GeometryException;
import com.onthegomap.planetiler.geo.GeometryType;
import com.onthegomap.planetiler.reader.SimpleFeature;
import com.onthegomap.planetiler.reader.Struct;
import com.onthegomap.planetiler.stats.Stats;
import com.onthegomap.planetiler.util.ZoomFunction;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.StreamSupport;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.locationtech.jts.geom.Geometry;

class FeatureCollectorTest {

  private final PlanetilerConfig config = PlanetilerConfig.defaults();
  private final FeatureCollector.Factory factory = new FeatureCollector.Factory(config, Stats.inMemory());

  private static void assertFeatures(int zoom, List<Map<String, Object>> expected, FeatureCollector actual) {
    List<FeatureCollector.Feature> actualList = StreamSupport.stream(actual.spliterator(), false).toList();
    assertEquals(expected.size(), actualList.size(), "size");
    for (int i = 0; i < expected.size(); i++) {
      assertSubmap(expected.get(i), TestUtils.toMap(actualList.get(i), zoom));
    }
  }

  private SimpleFeature newReaderFeature(Geometry latLonGeometry, Map<String, Object> tags) {
    return SimpleFeature.create(latLonGeometry, tags);
  }

  @Test
  void testEmpty() {
    var collector = factory.get(newReaderFeature(newPoint(0, 0), Map.of(
      "key", "val"
    )));
    assertFeatures(14, List.of(), collector);
  }

  @Test
  void testPoint() {
    var collector = factory.get(newReaderFeature(newPoint(0, 0), Map.of(
      "key", "val"
    )));
    collector.point("layername")
      .setZoomRange(12, 14)
      .setSortKey(3)
      .setAttr("attr1", 2)
      .setBufferPixels(10d)
      .setBufferPixelOverrides(ZoomFunction.maxZoom(12, 100d))
      .setPointLabelGridSizeAndLimit(12, 100, 10)
      .setId(123456789);
    assertFeatures(14, List.of(
      Map.of(
        "_layer", "layername",
        "_minzoom", 12,
        "_maxzoom", 14,
        "_sortkey", 3,
        "_labelgrid_size", 0d,
        "_labelgrid_limit", 0,
        "attr1", 2,
        "_type", "point",
        "_buffer", 10d,
        "_id", 123456789L
      )
    ), collector);
    assertFeatures(12, List.of(
      Map.of(
        "_labelgrid_size", 100d,
        "_labelgrid_limit", 10,
        "_buffer", 100d,
        "_id", 123456789L
      )
    ), collector);

  }

  @Test
  void testOmit() {
    var collector = factory.get(newReaderFeature(newPoint(0, 0), Map.of(
      "key", "val"
    )));
    var point = collector.point("layername");
    assertFeatures(14, List.of(Map.of("_layer", "layername")), collector);
    point.omit();
    assertFeatures(14, List.of(), collector);
  }

  @Test
  void testAttrWithMinzoom() {
    var collector = factory.get(newReaderFeature(newPoint(0, 0), Map.of(
      "key", "val"
    )));
    collector.point("layername")
      .setAttr("attr1", "value1")
      .setAttrWithMinzoom("attr2", "value2", 13);
    assertFeatures(13, List.of(
      Map.of(
        "attr1", "value1",
        "attr2", "value2"
      )
    ), collector);
    assertFeatures(12, List.of(
      Map.of(
        "attr1", "value1",
        "attr2", "<null>"
      )
    ), collector);
  }

  @Test
  void testLine() {
    var collector = factory.get(newReaderFeature(newLineString(
      0, 0,
      1, 1
    ), Map.of(
      "key", "val"
    )));
    collector.line("layername")
      .setZoomRange(12, 14)
      .setSortKey(3)
      .setAttr("attr1", 2)
      .setMinPixelSize(1)
      .setMinPixelSizeBelowZoom(12, 10);
    assertFeatures(13, List.of(
      Map.of(
        "_layer", "layername",
        "_minzoom", 12,
        "_maxzoom", 14,
        "_sortkey", 3,
        "_minpixelsize", 1d,
        "attr1", 2,
        "_type", "line"
      )
    ), collector);
  }

  @Test
  void testPolygon() {
    var collector = factory.get(newReaderFeature(newPolygon(
      0, 0,
      1, 0,
      1, 1,
      0, 1,
      0, 0
    ), Map.of(
      "key", "val"
    )));
    collector.polygon("layername")
      .setZoomRange(12, 14)
      .setSortKey(3)
      .setAttr("attr1", 2)
      .inheritAttrFromSource("key")
      .setMinPixelSize(1)
      .setMinPixelSizeBelowZoom(12, 10);
    assertFeatures(13, List.of(
      Map.of(
        "_layer", "layername",
        "_minzoom", 12,
        "_maxzoom", 14,
        "_sortkey", 3,
        "_minpixelsize", 1d,
        "attr1", 2,
        "_type", "polygon"
      )
    ), collector);
    assertFeatures(12, List.of(
      Map.of(
        "_minpixelsize", 10d
      )
    ), collector);
  }

  @Test
  void testMinSizeAtMaxZoomDefaultsToTileResolution() {
    var collector = factory.get(newReaderFeature(rectangle(10, 20), Map.of()));
    var poly = collector.polygon("layername")
      .setMinPixelSize(1)
      .setMinPixelSizeBelowZoom(12, 10);
    assertEquals(10, poly.getMinPixelSizeAtZoom(12));
    assertEquals(1, poly.getMinPixelSizeAtZoom(13));
    assertEquals(256d / 4096, poly.getMinPixelSizeAtZoom(14));
  }

  @Test
  void testSetMinSizeAtMaxZoom() {
    var collector = factory.get(newReaderFeature(rectangle(10, 20), Map.of()));
    var poly = collector.polygon("layername")
      .setMinPixelSize(1)
      .setMinPixelSizeAtMaxZoom(0.5)
      .setMinPixelSizeBelowZoom(12, 10);
    assertEquals(10, poly.getMinPixelSizeAtZoom(12));
    assertEquals(1, poly.getMinPixelSizeAtZoom(13));
    assertEquals(0.5, poly.getMinPixelSizeAtZoom(14));
  }

  @Test
  void testSetMinSizeAtAllZooms() {
    var collector = factory.get(newReaderFeature(rectangle(10, 20), Map.of()));
    var poly = collector.polygon("layername")
      .setMinPixelSizeAtAllZooms(2)
      .setMinPixelSizeBelowZoom(12, 10);
    assertEquals(10, poly.getMinPixelSizeAtZoom(12));
    assertEquals(2, poly.getMinPixelSizeAtZoom(13));
    assertEquals(2, poly.getMinPixelSizeAtZoom(14));
  }

  @Test
  void testDefaultMinPixelSize() {
    var collector = factory.get(newReaderFeature(rectangle(10, 20), Map.of()));
    var poly = collector.polygon("layername");
    assertEquals(1, poly.getMinPixelSizeAtZoom(12));
    assertEquals(1, poly.getMinPixelSizeAtZoom(13));
    assertEquals(256d / 4096, poly.getMinPixelSizeAtZoom(14));
  }

  @Test
  void testToleranceDefault() {
    var collector = factory.get(newReaderFeature(rectangle(10, 20), Map.of()));
    var poly = collector.polygon("layername");
    assertEquals(0.1, poly.getPixelToleranceAtZoom(12));
    assertEquals(0.1, poly.getPixelToleranceAtZoom(13));
    assertEquals(256d / 4096, poly.getPixelToleranceAtZoom(14));
  }

  @Test
  void testSetTolerance() {
    var collector = factory.get(newReaderFeature(rectangle(10, 20), Map.of()));
    var poly = collector.polygon("layername")
      .setPixelTolerance(1);
    assertEquals(1d, poly.getPixelToleranceAtZoom(12));
    assertEquals(1d, poly.getPixelToleranceAtZoom(13));
    assertEquals(256d / 4096, poly.getPixelToleranceAtZoom(14));
  }

  @Test
  void testSetToleranceAtAllZooms() {
    var collector = factory.get(newReaderFeature(rectangle(10, 20), Map.of()));
    var poly = collector.polygon("layername")
      .setPixelToleranceAtAllZooms(1);
    assertEquals(1d, poly.getPixelToleranceAtZoom(12));
    assertEquals(1d, poly.getPixelToleranceAtZoom(13));
    assertEquals(1d, poly.getPixelToleranceAtZoom(14));
  }

  @Test
  void testSetMaxZoom() {
    var collector = factory.get(newReaderFeature(rectangle(10, 20), Map.of()));
    var poly = collector.polygon("layername")
      .setPixelToleranceAtMaxZoom(2);
    assertEquals(0.1d, poly.getPixelToleranceAtZoom(12));
    assertEquals(0.1d, poly.getPixelToleranceAtZoom(13));
    assertEquals(2d, poly.getPixelToleranceAtZoom(14));
  }

  @Test
  void testSetAllZoomMethods() {
    var collector = factory.get(newReaderFeature(rectangle(10, 20), Map.of()));
    var poly = collector.polygon("layername")
      .setPixelTolerance(1)
      .setPixelToleranceAtMaxZoom(2)
      .setPixelToleranceBelowZoom(12, 3);
    assertEquals(3d, poly.getPixelToleranceAtZoom(12));
    assertEquals(1d, poly.getPixelToleranceAtZoom(13));
    assertEquals(2d, poly.getPixelToleranceAtZoom(14));
  }

  /*
   * SHAPE COERCION TESTS
   */
  @Test
  void testPointReaderFeatureCoercion() throws GeometryException {
    var pointSourceFeature = newReaderFeature(newPoint(0, 0), Map.of());
    assertEquals(0, pointSourceFeature.area());
    assertEquals(0, pointSourceFeature.length());

    var fc = factory.get(pointSourceFeature);
    fc.line("layer").setZoomRange(0, 10);
    fc.polygon("layer").setZoomRange(0, 10);
    assertFalse(fc.iterator().hasNext(), "silently fail coercing to line/polygon");
    fc.point("layer").setZoomRange(0, 10);
    fc.centroid("layer").setZoomRange(0, 10);
    fc.pointOnSurface("layer").setZoomRange(0, 10);
    var iter = fc.iterator();
    for (int i = 0; i < 3; i++) {
      assertTrue(iter.hasNext(), "item " + i);
      var item = iter.next();
      assertEquals(GeometryType.POINT, item.getGeometryType());
      assertEquals(newPoint(0.5, 0.5), item.getGeometry());
    }

    assertFalse(iter.hasNext());
  }

  @ParameterizedTest
  @ValueSource(ints = {2, 3, 4})
  void testLineWithSamePointsReaderFeatureCoercion(int nPoints) throws GeometryException {
    double[] coords = new double[nPoints * 2];
    Arrays.fill(coords, 0d);
    double[] worldCoords = new double[nPoints * 2];
    Arrays.fill(worldCoords, 0.5d);
    var sourceLine = newReaderFeature(newLineString(coords), Map.of());
    assertEquals(0, sourceLine.length());
    assertEquals(0, sourceLine.area());

    var fc = factory.get(sourceLine);
    fc.point("layer").setZoomRange(0, 10);
    fc.polygon("layer").setZoomRange(0, 10);
    assertFalse(fc.iterator().hasNext(), "silently fail coercing to point/polygon");
    fc.line("layer").setZoomRange(0, 10);
    fc.centroid("layer").setZoomRange(0, 10);
    fc.pointOnSurface("layer").setZoomRange(0, 10);
    var iter = fc.iterator();

    var item = iter.next();
    assertEquals(GeometryType.LINE, item.getGeometryType());
    assertEquals(newLineString(worldCoords), item.getGeometry());

    item = iter.next();
    assertEquals(GeometryType.POINT, item.getGeometryType());
    assertEquals(newPoint(0.5, 0.5), item.getGeometry());

    item = iter.next();
    assertEquals(GeometryType.POINT, item.getGeometryType());
    assertEquals(newPoint(0.5, 0.5), item.getGeometry());

    assertFalse(iter.hasNext());
  }

  private static double[] worldToLatLon(double... coords) {
    double[] result = new double[coords.length];
    for (int i = 0; i < coords.length; i += 2) {
      result[i] = GeoUtils.getWorldLon(coords[i]);
      result[i + 1] = GeoUtils.getWorldLat(coords[i + 1]);
    }
    return result;
  }

  @Test
  void testNonZeroLineStringReaderFeatureCoercion() throws GeometryException {
    var sourceLine = newReaderFeature(newLineString(worldToLatLon(
      0.2, 0.2,
      0.75, 0.75,
      0.25, 0.75,
      0.2, 0.2
    )), Map.of());
    assertEquals(0, sourceLine.area());
    assertEquals(1.83008, sourceLine.length(), 1e-5);

    var fc = factory.get(sourceLine);
    fc.point("layer").setZoomRange(0, 10);
    fc.polygon("layer").setZoomRange(0, 10);

    assertFalse(fc.iterator().hasNext(), "silently fail coercing to point/polygon");
    fc.line("layer").setZoomRange(0, 10);
    fc.centroid("layer").setZoomRange(0, 10);
    fc.pointOnSurface("layer").setZoomRange(0, 10);
    var iter = fc.iterator();

    var item = iter.next();
    assertEquals(GeometryType.LINE, item.getGeometryType());
    assertEquals(round(newLineString(
      0.2, 0.2,
      0.75, 0.75,
      0.25, 0.75,
      0.2, 0.2
    )), round(item.getGeometry()));

    item = iter.next();
    assertEquals(GeometryType.POINT, item.getGeometryType());
    assertEquals(round(newPoint(0.40639, 0.55013)), round(item.getGeometry()), "centroid");

    item = iter.next();
    assertEquals(GeometryType.POINT, item.getGeometryType());
    assertEquals(newPoint(0.25, 0.75), item.getGeometry(), "point on surface");

    assertFalse(iter.hasNext());
  }

  @Test
  void testPolygonReaderFeatureCoercion() throws GeometryException {
    var sourceLine = newReaderFeature(newPolygon(worldToLatLon(
      0.25, 0.25,
      0.75, 0.75,
      0.25, 0.75,
      0.25, 0.25
    )), Map.of());
    assertEquals(0.125, sourceLine.area());
    assertEquals(1.7071067811865475, sourceLine.length(), 1e-5);

    var fc = factory.get(sourceLine);
    fc.point("layer").setZoomRange(0, 10);
    fc.line("layer").setZoomRange(0, 10);
    assertFalse(fc.iterator().hasNext(), "silently fail coercing to point or line");

    fc.polygon("layer").setZoomRange(0, 10);
    fc.centroid("layer").setZoomRange(0, 10);
    fc.pointOnSurface("layer").setZoomRange(0, 10);
    var iter = fc.iterator();

    var item = iter.next();
    assertEquals(GeometryType.POLYGON, item.getGeometryType());
    assertEquals(round(newPolygon(
      0.25, 0.25,
      0.75, 0.75,
      0.25, 0.75,
      0.25, 0.25
    )), round(item.getGeometry()));

    item = iter.next();
    assertEquals(GeometryType.POINT, item.getGeometryType());
    assertEquals(round(newPoint(0.4166667, 0.5833333)), round(item.getGeometry()));

    item = iter.next();
    assertEquals(GeometryType.POINT, item.getGeometryType());
    assertEquals(round(newPoint(0.375, 0.5)), round(item.getGeometry()));

    assertFalse(iter.hasNext());
  }

  @Test
  void testPolygonWithHoleCoercion() throws GeometryException {
    var sourceLine = newReaderFeature(newPolygon(newCoordinateList(worldToLatLon(
      0, 0,
      1, 0,
      1, 1,
      0, 1,
      0, 0
    )), List.of(newCoordinateList(worldToLatLon(
      0.25, 0.25,
      0.75, 0.25,
      0.75, 0.75,
      0.25, 0.75,
      0.25, 0.25
    )))), Map.of());
    assertEquals(0.75, sourceLine.area(), 1e-5);
    assertEquals(6, sourceLine.length(), 1e-5);

    var fc = factory.get(sourceLine);
    fc.point("layer").setZoomRange(0, 10);
    fc.line("layer").setZoomRange(0, 10);
    assertFalse(fc.iterator().hasNext(), "silently fail coercing to point or line");

    fc.polygon("layer").setZoomRange(0, 10);
    fc.centroid("layer").setZoomRange(0, 10);
    fc.pointOnSurface("layer").setZoomRange(0, 10);
    var iter = fc.iterator();

    var item = iter.next();
    assertEquals(GeometryType.POLYGON, item.getGeometryType());
    assertEquals(round(newPolygon(
      rectangleCoordList(0, 1),
      List.of(rectangleCoordList(0.25, 0.75))
    )), round(item.getGeometry()));

    item = iter.next();
    assertEquals(GeometryType.POINT, item.getGeometryType());
    assertEquals(round(newPoint(0.5, 0.5)), round(item.getGeometry()));

    item = iter.next();
    assertEquals(GeometryType.POINT, item.getGeometryType());
    assertEquals(round(newPoint(0.125, 0.5)), round(item.getGeometry()));

    assertFalse(iter.hasNext());
  }

  @Test
  void testPointOnSurface() {
    var sourceLine = newReaderFeature(newPolygon(worldToLatLon(
      0, 0,
      1, 0,
      1, 0.25,
      0.25, 0.25,
      0.25, 0.75,
      1, 0.75,
      1, 1,
      0, 1,
      0, 0
    )), Map.of());

    var fc = factory.get(sourceLine);
    fc.centroid("layer").setZoomRange(0, 10);
    fc.pointOnSurface("layer").setZoomRange(0, 10);
    var iter = fc.iterator();

    var item = iter.next();
    assertEquals(GeometryType.POINT, item.getGeometryType());
    assertEquals(round(newPoint(0.425, 0.5)), round(item.getGeometry()));

    item = iter.next();
    assertEquals(GeometryType.POINT, item.getGeometryType());
    assertEquals(round(newPoint(0.125, 0.5)), round(item.getGeometry()));

    assertFalse(iter.hasNext());
  }

  @Test
  void testInnermostPoint() {
    /*
      _____
     | · __|
     |__|
     */
    var sourceLine = newReaderFeature(newPolygon(worldToLatLon(
      0, 0,
      1, 0,
      1, 0.5,
      0.5, 0.5,
      0.5, 1,
      0, 1,
      0, 0
    )), Map.of());

    var fc = factory.get(sourceLine);
    fc.innermostPoint("layer").setZoomRange(0, 10);
    var iter = fc.iterator();

    var item = iter.next();
    assertEquals(GeometryType.POINT, item.getGeometryType());
    assertEquals(round(newPoint(0.28, 0.28)), round(item.getGeometry(), 1e2));

    assertFalse(iter.hasNext());
  }

  @Test
  void testMultiPolygonCoercion() throws GeometryException {
    var sourceLine = newReaderFeature(newMultiPolygon(
      newPolygon(worldToLatLon(
        0, 0,
        1, 0,
        1, 1,
        0, 1,
        0, 0
      )), newPolygon(worldToLatLon(
        2, 0,
        3, 0,
        3, 1,
        2, 1,
        2, 0
      ))), Map.of());
    assertEquals(2, sourceLine.area(), 1e-5);
    assertEquals(8, sourceLine.length(), 1e-5);

    var fc = factory.get(sourceLine);
    fc.point("layer").setZoomRange(0, 10);
    fc.line("layer").setZoomRange(0, 10);
    assertFalse(fc.iterator().hasNext(), "silently fail coercing to point or line");

    fc.polygon("layer").setZoomRange(0, 10);
    fc.line("layer").setZoomRange(0, 10);
    fc.centroid("layer").setZoomRange(0, 10);
    fc.pointOnSurface("layer").setZoomRange(0, 10);
    var iter = fc.iterator();

    var item = iter.next();
    assertEquals(GeometryType.POLYGON, item.getGeometryType());
    assertEquals(round(newMultiPolygon(
      rectangle(0, 1),
      rectangle(2, 0, 3, 1)
    )), round(item.getGeometry()));

    item = iter.next();
    assertEquals(GeometryType.POINT, item.getGeometryType());
    assertEquals(round(newPoint(1.5, 0.5)), round(item.getGeometry()));

    item = iter.next();
    assertEquals(GeometryType.POINT, item.getGeometryType());
    assertEquals(round(newPoint(0.5, 0.5)), round(item.getGeometry()));

    assertFalse(iter.hasNext());
  }

  @Test
  void testMultiLineStringCoercion() throws GeometryException {
    var sourceLine = newReaderFeature(newMultiLineString(
      newLineString(worldToLatLon(
        0, 0,
        1, 0,
        1, 1,
        0, 1,
        0, 0
      )), newLineString(worldToLatLon(
        2, 0,
        3, 0,
        3, 1,
        2, 1,
        2, 0
      ))), Map.of());
    assertEquals(0, sourceLine.area(), 1e-5);
    assertEquals(8, sourceLine.length(), 1e-5);

    var fc = factory.get(sourceLine);
    fc.point("layer").setZoomRange(0, 10);
    fc.polygon("layer").setZoomRange(0, 10);
    assertFalse(fc.iterator().hasNext(), "silently fail coercing to point/polygon");

    fc.line("layer").setZoomRange(0, 10);
    fc.centroid("layer").setZoomRange(0, 10);
    fc.pointOnSurface("layer").setZoomRange(0, 10);
    var iter = fc.iterator();

    var item = iter.next();
    assertEquals(GeometryType.LINE, item.getGeometryType());
    assertEquals(round(newMultiLineString(
      newLineString(rectangleCoordList(0, 1)),
      newLineString(rectangleCoordList(2, 0, 3, 1))
    )), round(item.getGeometry()));

    item = iter.next();
    assertEquals(GeometryType.POINT, item.getGeometryType());
    assertEquals(round(newPoint(1.5, 0.5)), round(item.getGeometry()));

    item = iter.next();
    assertEquals(GeometryType.POINT, item.getGeometryType());
    assertEquals(round(newPoint(1, 0)), round(item.getGeometry()));

    assertFalse(iter.hasNext());
  }

  @Test
  void testManyAttr() {

    Map<String, Object> tags = new HashMap<>();

    for (int i = 0; i < 500; i++) {
      tags.put("key" + i, "val" + i);
    }

    var collector = factory.get(newReaderFeature(newPoint(0, 0), tags));
    var point = collector.point("layername");

    for (int i = 0; i < 500; i++) {
      point.setAttr("key" + i, tags.get("key" + i));
    }

    assertFeatures(13, List.of(
      Map.of(
        "key0", "val0",
        "key10", "val10",
        "key100", "val100",
        "key256", "val256",
        "key499", "val499"
      )
    ), collector);
  }

  @Test
  void testPartialLineFeature() {
    var collector = factory.get(newReaderFeature(newLineString(worldToLatLon(0, 0, 1, 0)), Map.of()));
    collector.partialLine("layername", 0.25, 0.5).setAttr("k1", "v1");
    collector.partialLine("layername", 0.75, 1).setAttr("k2", "v2");
    assertFeatures(14, List.of(
      Map.of(
        "_geom", new RoundGeometry(newLineString(0.25, 0, 0.5, 0)),
        "k1", "v1",
        "k2", "<null>"
      ),
      Map.of(
        "_geom", new RoundGeometry(newLineString(0.75, 0, 1, 0)),
        "k1", "<null>",
        "k2", "v2"
      )
    ), collector);
  }

  @Test
  void testLinearReferenceTags() {
    var collector = factory.get(newReaderFeature(newLineString(worldToLatLon(0, 0, 1, 0)), Map.of()));
    collector.line("layername")
      .linearRange(0.1, 0.5).setAttr("k1", "v1")
      .linearRange(0.3, 0.7).setAttr("k2", "v2")
      .entireLine().setAttr("k3", "v3");

    var feature = collector.iterator().next();
    assertTrue(feature.hasLinearRanges());
    assertEquals(List.of(
      new FeatureCollector.RangeWithTags(0, 0.1, roundTrip(newLineString(0, 0, 0.1, 0)), Map.of(
        "k3", "v3"
      )),
      new FeatureCollector.RangeWithTags(0.1, 0.3, roundTrip(newLineString(0.1, 0, 0.3, 0)), Map.of(
        "k1", "v1",
        "k3", "v3"
      )),
      new FeatureCollector.RangeWithTags(0.3, 0.5, roundTrip(newLineString(0.3, 0, 0.5, 0)), Map.of(
        "k1", "v1",
        "k2", "v2",
        "k3", "v3"
      )),
      new FeatureCollector.RangeWithTags(0.5, 0.7, roundTrip(newLineString(0.5, 0, 0.7, 0)), Map.of(
        "k2", "v2",
        "k3", "v3"
      )),
      new FeatureCollector.RangeWithTags(0.7, 1, roundTrip(newLineString(0.7, 0, 1, 0)), Map.of(
        "k3", "v3"
      ))
    ), feature.getLinearRangesAtZoom(14));
  }

  @Test
  void testPartialMinzoom() {
    var collector = factory.get(newReaderFeature(newLineString(worldToLatLon(0, 0, 1, 0)), Map.of()));
    collector.line("layername")
      .linearRange(0.25, 0.75).setMinZoom(14);
    assertFeatures(13, List.of(
      Map.of("_geom", new RoundGeometry(newLineString(0, 0, 1, 0)))
    ), collector);
    assertFeatures(14, List.of(
      Map.of("_geom", new RoundGeometry(newLineString(0, 0, 1, 0)))
    ), collector);
    var feature = collector.iterator().next();
    assertTrue(feature.hasLinearRanges());
    assertEquals(List.of(
      new FeatureCollector.RangeWithTags(0, 0.25, roundTrip(newLineString(0, 0, 0.25, 0)), Map.of()),
      new FeatureCollector.RangeWithTags(0.75, 1, roundTrip(newLineString(0.75, 0, 1, 0)), Map.of())
    ), feature.getLinearRangesAtZoom(13));
    assertEquals(List.of(
      new FeatureCollector.RangeWithTags(0, 1, roundTrip(newLineString(0, 0, 1, 0)), Map.of())
    ), feature.getLinearRangesAtZoom(14));
  }

  private static Geometry roundTrip(Geometry world) {
    return GeoUtils.latLonToWorldCoords(GeoUtils.worldToLatLonCoords(world));
  }

  @Test
  void testPartialOmit() {
    var collector = factory.get(newReaderFeature(newLineString(worldToLatLon(0, 0, 1, 0)), Map.of()));
    collector.line("layername")
      .linearRange(0.25, 0.75).omit();
    var feature = collector.iterator().next();
    assertTrue(feature.hasLinearRanges());
    assertEquals(List.of(
      new FeatureCollector.RangeWithTags(0, 0.25, roundTrip(newLineString(0, 0, 0.25, 0)), Map.of()),
      new FeatureCollector.RangeWithTags(0.75, 1, roundTrip(newLineString(0.75, 0, 1, 0)), Map.of())
    ), feature.getLinearRangesAtZoom(13));
    assertEquals(List.of(
      new FeatureCollector.RangeWithTags(0, 0.25, roundTrip(newLineString(0, 0, 0.25, 0)), Map.of()),
      new FeatureCollector.RangeWithTags(0.75, 1, roundTrip(newLineString(0.75, 0, 1, 0)), Map.of())
    ), feature.getLinearRangesAtZoom(14));
  }

  @Test
  void testSetAttrStruct() {
    var collector = factory.get(newReaderFeature(newPoint(0, 0), Map.of()));
    collector.point("layername")
      .setAttr("a", Struct.of(1))
      .setAttrWithMinzoom("b", Struct.of(2d), 9)
      .putAttrs(Map.of("c", Struct.of("3"), "d", ZoomFunction.minZoom(9, Struct.of(true))));
    var feature = collector.iterator().next();
    assertEquals(Map.of(
      "a", 1,
      "b", 2d,
      "c", "3",
      "d", true
    ), feature.getAttrsAtZoom(14));
  }

  @Test
  void testSetAttrPartial() {
    var collector = factory.get(newReaderFeature(newLineString(0, 0, 1, 1), Map.of()));
    collector.line("layername")
      .linearRange(0, 0.5)
      .setAttr("a", Struct.of(1))
      .setAttrWithMinzoom("b", Struct.of(2d), 9)
      .putAttrs(Map.of("c", Struct.of("3"), "d", ZoomFunction.minZoom(9, Struct.of(true))));
    var feature = collector.iterator().next();
    var subFeature = feature.getLinearRangesAtZoom(14).getFirst();
    assertEquals(Map.of(
      "a", 1,
      "b", 2d,
      "c", "3",
      "d", true
    ), subFeature.attrs());
  }

  @Test
  void testSetAttrPartialWithMinzoom() {
    var collector = factory.get(newReaderFeature(newLineString(0, 0, 1, 1), Map.of()));
    collector.line("layername")
      .setAttrWithMinzoom("full", 1, 2)
      .setAttrWithMinzoom("fullstruct", Struct.of(2), 2)
      .linearRange(0, 0.5)
      .setAttr("a", Struct.of(1))
      .setAttrWithMinzoom("b", Struct.of(2d), 9)
      .putAttrs(Map.of("c", Struct.of("3"), "d", ZoomFunction.minZoom(9, Struct.of(true))));
    var feature = collector.iterator().next();
    var subFeature = feature.getLinearRangesAtZoom(14).getFirst();
    assertEquals(Map.of(
      "a", 1,
      "b", 2d,
      "c", "3",
      "d", true,
      "full", 1,
      "fullstruct", 2
    ), subFeature.attrs());
  }

  @Test
  void testUnwrapStruct() {
    var collector = factory.get(newReaderFeature(newLineString(0, 0, 1, 1), Map.of()));
    collector.line("layername")
      .setAttr("full", Struct.of(1))
      .linearRange(0, 0.5)
      .setAttr("partial", Struct.of(2));
    var feature = collector.iterator().next();
    var subFeature = feature.getLinearRangesAtZoom(14).getFirst();
    assertEquals(Map.of(
      "full", 1,
      "partial", 2
    ), subFeature.attrs());
  }

  @Test
  void testUnwrapStructFull() {
    var collector = factory.get(newReaderFeature(newLineString(0, 0, 1, 1), Map.of()));
    collector.line("layername")
      .setAttr("full", Struct.of(1));
    var feature = collector.iterator().next();
    assertEquals(Map.of(
      "full", 1
    ), feature.getAttrsAtZoom(14));
  }

  @Test
  void testUnwrapStructFullWithMinzoom() {
    var collector = factory.get(newReaderFeature(newLineString(0, 0, 1, 1), Map.of()));
    collector.line("layername")
      .setAttrWithMinzoom("full", Struct.of(1), 2);
    var feature = collector.iterator().next();
    assertEquals(Map.of(
      "full", 1
    ), feature.getAttrsAtZoom(14));
  }

  @Test
  void testSetAttrPartialWithMinSize() {
    var collector = factory.get(newReaderFeature(newLineString(0, 0, 1, 1), Map.of()));
    var line = collector.line("layername");

    assertEquals(7, line.getMinZoomForPixelSize(100));
    assertEquals(7, line.linearRange(0, 0.5).getMinZoomForPixelSize(50));
    assertEquals(7, line.linearRange(0, 0.25).getMinZoomForPixelSize(25));
  }
}
