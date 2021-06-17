package com.onthegomap.flatmap;

import static com.onthegomap.flatmap.TestUtils.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.onthegomap.flatmap.geo.GeoUtils;
import com.onthegomap.flatmap.geo.GeometryException;
import com.onthegomap.flatmap.monitoring.Stats;
import com.onthegomap.flatmap.read.ReaderFeature;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.StreamSupport;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.locationtech.jts.geom.Geometry;

public class FeatureCollectorTest {

  private CommonParams config = CommonParams.defaults();
  private FeatureCollector.Factory factory = new FeatureCollector.Factory(config, new Stats.InMemory());

  private static void assertFeatures(int zoom, List<Map<String, Object>> expected, FeatureCollector actual) {
    List<FeatureCollector.Feature> actualList = StreamSupport.stream(actual.spliterator(), false).toList();
    assertEquals(expected.size(), actualList.size(), "size");
    for (int i = 0; i < expected.size(); i++) {
      assertSubmap(expected.get(i), TestUtils.toMap(actualList.get(i), zoom));
    }
  }

  private long id = 0;

  private ReaderFeature newReaderFeature(Geometry latLonGeometry, Map<String, Object> properties) {
    return new ReaderFeature(latLonGeometry, properties, id++);
  }

  @Test
  public void testEmpty() {
    var collector = factory.get(newReaderFeature(newPoint(0, 0), Map.of(
      "key", "val"
    )));
    assertFeatures(14, List.of(), collector);
  }

  @Test
  public void testPoint() {
    var collector = factory.get(newReaderFeature(newPoint(0, 0), Map.of(
      "key", "val"
    )));
    collector.point("layername")
      .setZoomRange(12, 14)
      .setZorder(3)
      .setAttr("attr1", 2)
      .setBufferPixels(10d)
      .setBufferPixelOverrides(ZoomFunction.maxZoom(12, 11d))
      .setLabelGridSizeAndLimit(12, 100, 10);
    assertFeatures(14, List.of(
      Map.of(
        "_layer", "layername",
        "_minzoom", 12,
        "_maxzoom", 14,
        "_zorder", 3,
        "_labelgrid_size", 0d,
        "_labelgrid_limit", 0,
        "attr1", 2,
        "_type", "point",
        "_buffer", 10d
      )
    ), collector);
    assertFeatures(12, List.of(
      Map.of(
        "_labelgrid_size", 100d,
        "_labelgrid_limit", 10,
        "_buffer", 11d
      )
    ), collector);
  }

  @Test
  public void testAttrWithMinzoom() {
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
  public void testLine() {
    var collector = factory.get(newReaderFeature(newLineString(
      0, 0,
      1, 1
    ), Map.of(
      "key", "val"
    )));
    collector.line("layername")
      .setZoomRange(12, 14)
      .setZorder(3)
      .setAttr("attr1", 2)
      .setMinPixelSize(1)
      .setMinPixelSizeBelowZoom(12, 10);
    assertFeatures(13, List.of(
      Map.of(
        "_layer", "layername",
        "_minzoom", 12,
        "_maxzoom", 14,
        "_zorder", 3,
        "_minpixelsize", 1d,
        "attr1", 2,
        "_type", "line"
      )
    ), collector);
  }

  @Test
  public void testPolygon() {
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
      .setZorder(3)
      .setAttr("attr1", 2)
      .inheritFromSource("key")
      .setMinPixelSize(1)
      .setMinPixelSizeBelowZoom(12, 10);
    assertFeatures(13, List.of(
      Map.of(
        "_layer", "layername",
        "_minzoom", 12,
        "_maxzoom", 14,
        "_zorder", 3,
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
  public void testMinSizeAtMaxZoomDefaultsToTileResolution() {
    var collector = factory.get(newReaderFeature(rectangle(10, 20), Map.of()));
    var poly = collector.polygon("layername")
      .setMinPixelSize(1)
      .setMinPixelSizeBelowZoom(12, 10);
    assertEquals(10, poly.getMinPixelSize(12));
    assertEquals(1, poly.getMinPixelSize(13));
    assertEquals(256d / 4096, poly.getMinPixelSize(14));
  }

  @Test
  public void testSetMinSizeAtMaxZoom() {
    var collector = factory.get(newReaderFeature(rectangle(10, 20), Map.of()));
    var poly = collector.polygon("layername")
      .setMinPixelSize(1)
      .setMinPixelSizeAtMaxZoom(0.5)
      .setMinPixelSizeBelowZoom(12, 10);
    assertEquals(10, poly.getMinPixelSize(12));
    assertEquals(1, poly.getMinPixelSize(13));
    assertEquals(0.5, poly.getMinPixelSize(14));
  }

  @Test
  public void testSetMinSizeAtAllZooms() {
    var collector = factory.get(newReaderFeature(rectangle(10, 20), Map.of()));
    var poly = collector.polygon("layername")
      .setMinPixelSizeAtAllZooms(2)
      .setMinPixelSizeBelowZoom(12, 10);
    assertEquals(10, poly.getMinPixelSize(12));
    assertEquals(2, poly.getMinPixelSize(13));
    assertEquals(2, poly.getMinPixelSize(14));
  }

  @Test
  public void testDefaultMinPixelSize() {
    var collector = factory.get(newReaderFeature(rectangle(10, 20), Map.of()));
    var poly = collector.polygon("layername");
    assertEquals(1, poly.getMinPixelSize(12));
    assertEquals(1, poly.getMinPixelSize(13));
    assertEquals(256d / 4096, poly.getMinPixelSize(14));
  }

  @Test
  public void testToleranceDefault() {
    var collector = factory.get(newReaderFeature(rectangle(10, 20), Map.of()));
    var poly = collector.polygon("layername");
    assertEquals(0.1, poly.getPixelTolerance(12));
    assertEquals(0.1, poly.getPixelTolerance(13));
    assertEquals(256d / 4096, poly.getPixelTolerance(14));
  }

  @Test
  public void testSetTolerance() {
    var collector = factory.get(newReaderFeature(rectangle(10, 20), Map.of()));
    var poly = collector.polygon("layername")
      .setPixelTolerance(1);
    assertEquals(1d, poly.getPixelTolerance(12));
    assertEquals(1d, poly.getPixelTolerance(13));
    assertEquals(256d / 4096, poly.getPixelTolerance(14));
  }

  @Test
  public void testSetToleranceAtAllZooms() {
    var collector = factory.get(newReaderFeature(rectangle(10, 20), Map.of()));
    var poly = collector.polygon("layername")
      .setPixelToleranceAtAllZooms(1);
    assertEquals(1d, poly.getPixelTolerance(12));
    assertEquals(1d, poly.getPixelTolerance(13));
    assertEquals(1d, poly.getPixelTolerance(14));
  }

  @Test
  public void testSetMaxZoom() {
    var collector = factory.get(newReaderFeature(rectangle(10, 20), Map.of()));
    var poly = collector.polygon("layername")
      .setPixelToleranceAtMaxZoom(2);
    assertEquals(0.1d, poly.getPixelTolerance(12));
    assertEquals(0.1d, poly.getPixelTolerance(13));
    assertEquals(2d, poly.getPixelTolerance(14));
  }

  @Test
  public void testSetAllZoomMethods() {
    var collector = factory.get(newReaderFeature(rectangle(10, 20), Map.of()));
    var poly = collector.polygon("layername")
      .setPixelTolerance(1)
      .setPixelToleranceAtMaxZoom(2)
      .setPixelToleranceBelowZoom(12, 3);
    assertEquals(3d, poly.getPixelTolerance(12));
    assertEquals(1d, poly.getPixelTolerance(13));
    assertEquals(2d, poly.getPixelTolerance(14));
  }

  /*
   * SHAPE COERCION TESTS
   */
  @Test
  public void testPointReaderFeatureCoercion() throws GeometryException {
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
  public void testLineWithSamePointsReaderFeatureCoercion(int nPoints) throws GeometryException {
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
  public void testNonZeroLineStringReaderFeatureCoercion() throws GeometryException {
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
  public void testPolygonReaderFeatureCoercion() throws GeometryException {
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
  public void testPolygonWithHoleCoercion() throws GeometryException {
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
  public void testPointOnSurface() {
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
  public void testMultiPolygonCoercion() throws GeometryException {
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
  public void testMultiLineStringCoercion() throws GeometryException {
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
}
