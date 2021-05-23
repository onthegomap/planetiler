package com.onthegomap.flatmap;

import static com.onthegomap.flatmap.TestUtils.assertSubmap;
import static com.onthegomap.flatmap.TestUtils.newLineString;
import static com.onthegomap.flatmap.TestUtils.newPoint;
import static com.onthegomap.flatmap.TestUtils.newPolygon;
import static com.onthegomap.flatmap.TestUtils.rectangle;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.onthegomap.flatmap.read.ReaderFeature;
import java.util.List;
import java.util.Map;
import java.util.stream.StreamSupport;
import org.junit.jupiter.api.Test;

public class FeatureCollectorTest {

  private CommonParams config = CommonParams.defaults();
  private FeatureCollector.Factory factory = new FeatureCollector.Factory(config);

  private static void assertFeatures(int zoom, List<Map<String, Object>> expected, FeatureCollector actual) {
    List<FeatureCollector.Feature> actualList = StreamSupport.stream(actual.spliterator(), false).toList();
    assertEquals(expected.size(), actualList.size(), "size");
    for (int i = 0; i < expected.size(); i++) {
      assertSubmap(expected.get(i), TestUtils.toMap(actualList.get(i), zoom));
    }
  }

  @Test
  public void testEmpty() {
    var collector = factory.get(new ReaderFeature(newPoint(0, 0), Map.of(
      "key", "val"
    )));
    assertFeatures(14, List.of(), collector);
  }

  @Test
  public void testPoint() {
    var collector = factory.get(new ReaderFeature(newPoint(0, 0), Map.of(
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
    var collector = factory.get(new ReaderFeature(newPoint(0, 0), Map.of(
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
    var collector = factory.get(new ReaderFeature(newLineString(
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
    var collector = factory.get(new ReaderFeature(newPolygon(
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
    var collector = factory.get(new ReaderFeature(rectangle(10, 20), Map.of()));
    var poly = collector.polygon("layername")
      .setMinPixelSize(1)
      .setMinPixelSizeBelowZoom(12, 10);
    assertEquals(10, poly.getMinPixelSize(12));
    assertEquals(1, poly.getMinPixelSize(13));
    assertEquals(256d / 4096, poly.getMinPixelSize(14));
  }

  @Test
  public void testSetMinSizeAtMaxZoom() {
    var collector = factory.get(new ReaderFeature(rectangle(10, 20), Map.of()));
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
    var collector = factory.get(new ReaderFeature(rectangle(10, 20), Map.of()));
    var poly = collector.polygon("layername")
      .setMinPixelSizeAtAllZooms(2)
      .setMinPixelSizeBelowZoom(12, 10);
    assertEquals(10, poly.getMinPixelSize(12));
    assertEquals(2, poly.getMinPixelSize(13));
    assertEquals(2, poly.getMinPixelSize(14));
  }

  @Test
  public void testDefaultMinPixelSize() {
    var collector = factory.get(new ReaderFeature(rectangle(10, 20), Map.of()));
    var poly = collector.polygon("layername");
    assertEquals(1, poly.getMinPixelSize(12));
    assertEquals(1, poly.getMinPixelSize(13));
    assertEquals(256d / 4096, poly.getMinPixelSize(14));
  }

  @Test
  public void testToleranceDefault() {
    var collector = factory.get(new ReaderFeature(rectangle(10, 20), Map.of()));
    var poly = collector.polygon("layername");
    assertEquals(0.1, poly.getPixelTolerance(12));
    assertEquals(0.1, poly.getPixelTolerance(13));
    assertEquals(256d / 4096, poly.getPixelTolerance(14));
  }

  @Test
  public void testSetTolerance() {
    var collector = factory.get(new ReaderFeature(rectangle(10, 20), Map.of()));
    var poly = collector.polygon("layername")
      .setPixelTolerance(1);
    assertEquals(1d, poly.getPixelTolerance(12));
    assertEquals(1d, poly.getPixelTolerance(13));
    assertEquals(256d / 4096, poly.getPixelTolerance(14));
  }

  @Test
  public void testSetToleranceAtAllZooms() {
    var collector = factory.get(new ReaderFeature(rectangle(10, 20), Map.of()));
    var poly = collector.polygon("layername")
      .setPixelToleranceAtAllZooms(1);
    assertEquals(1d, poly.getPixelTolerance(12));
    assertEquals(1d, poly.getPixelTolerance(13));
    assertEquals(1d, poly.getPixelTolerance(14));
  }

  @Test
  public void testSetMaxZoom() {
    var collector = factory.get(new ReaderFeature(rectangle(10, 20), Map.of()));
    var poly = collector.polygon("layername")
      .setPixelToleranceAtMaxZoom(2);
    assertEquals(0.1d, poly.getPixelTolerance(12));
    assertEquals(0.1d, poly.getPixelTolerance(13));
    assertEquals(2d, poly.getPixelTolerance(14));
  }

  @Test
  public void testSetAllZoomMethods() {
    var collector = factory.get(new ReaderFeature(rectangle(10, 20), Map.of()));
    var poly = collector.polygon("layername")
      .setPixelTolerance(1)
      .setPixelToleranceAtMaxZoom(2)
      .setPixelToleranceBelowZoom(12, 3);
    assertEquals(3d, poly.getPixelTolerance(12));
    assertEquals(1d, poly.getPixelTolerance(13));
    assertEquals(2d, poly.getPixelTolerance(14));
  }

  // TODO test shape coercion
}
