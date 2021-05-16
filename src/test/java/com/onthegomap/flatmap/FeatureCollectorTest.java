package com.onthegomap.flatmap;

import static com.onthegomap.flatmap.TestUtils.assertSubmap;
import static com.onthegomap.flatmap.TestUtils.newLineString;
import static com.onthegomap.flatmap.TestUtils.newPoint;
import static com.onthegomap.flatmap.TestUtils.newPolygon;
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
    List<FeatureCollector.Feature<?>> actualList = StreamSupport.stream(actual.spliterator(), false).toList();
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
      .setMinLength(1)
      .setMinLengthBelowZoom(12, 10);
    assertFeatures(14, List.of(
      Map.of(
        "_layer", "layername",
        "_minzoom", 12,
        "_maxzoom", 14,
        "_zorder", 3,
        "_minlength", 1d,
        "attr1", 2,
        "_type", "line"
      )
    ), collector);
    assertFeatures(12, List.of(
      Map.of(
        "_minlength", 10d
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
      .setMinArea(1)
      .setMinAreaBelowZoom(12, 10);
    assertFeatures(14, List.of(
      Map.of(
        "_layer", "layername",
        "_minzoom", 12,
        "_maxzoom", 14,
        "_zorder", 3,
        "_minarea", 1d,
        "attr1", 2,
        "_type", "polygon"
      )
    ), collector);
    assertFeatures(12, List.of(
      Map.of(
        "_minarea", 10d
      )
    ), collector);
  }
}
