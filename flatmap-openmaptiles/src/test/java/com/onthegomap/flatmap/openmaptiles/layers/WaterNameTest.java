package com.onthegomap.flatmap.openmaptiles.layers;

import static com.onthegomap.flatmap.TestUtils.newLineString;
import static com.onthegomap.flatmap.TestUtils.rectangle;
import static com.onthegomap.flatmap.openmaptiles.OpenMapTilesProfile.LAKE_CENTERLINE_SOURCE;
import static com.onthegomap.flatmap.openmaptiles.OpenMapTilesProfile.NATURAL_EARTH_SOURCE;
import static com.onthegomap.flatmap.openmaptiles.OpenMapTilesProfile.OSM_SOURCE;

import com.onthegomap.flatmap.TestUtils;
import com.onthegomap.flatmap.geo.GeoUtils;
import com.onthegomap.flatmap.read.ReaderFeature;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

public class WaterNameTest extends AbstractLayerTest {

  @Test
  public void testWaterNamePoint() {
    assertFeatures(11, List.of(Map.of(
      "_layer", "water"
    ), Map.of(
      "class", "lake",
      "name", "waterway",
      "name:es", "waterway es",
      "intermittent", 1,

      "_layer", "water_name",
      "_type", "point",
      "_minzoom", 9,
      "_maxzoom", 14
    )), process(polygonFeatureWithArea(1, Map.of(
      "name", "waterway",
      "name:es", "waterway es",
      "natural", "water",
      "water", "pond",
      "intermittent", "1"
    ))));
    double z11area = Math.pow((GeoUtils.metersToPixelAtEquator(0, Math.sqrt(70_000)) / 256d), 2) * Math.pow(2, 20 - 11);
    assertFeatures(10, List.of(Map.of(
      "_layer", "water"
    ), Map.of(
      "_layer", "water_name",
      "_type", "point",
      "_minzoom", 11,
      "_maxzoom", 14
    )), process(polygonFeatureWithArea(z11area, Map.of(
      "name", "waterway",
      "natural", "water",
      "water", "pond"
    ))));
  }

  @Test
  public void testWaterNameLakeline() {
    assertFeatures(11, List.of(), process(new ReaderFeature(
      newLineString(0, 0, 1, 1),
      new HashMap<>(Map.<String, Object>of(
        "OSM_ID", -10
      )),
      LAKE_CENTERLINE_SOURCE,
      null,
      0
    )));
    assertFeatures(10, List.of(Map.of(
      "_layer", "water"
    ), Map.of(
      "name", "waterway",
      "name:es", "waterway es",

      "_layer", "water_name",
      "_type", "line",
      "_geom", new TestUtils.NormGeometry(GeoUtils.latLonToWorldCoords(newLineString(0, 0, 1, 1))),
      "_minzoom", 9,
      "_maxzoom", 14,
      "_minpixelsize", "waterway".length() * 6d
    )), process(new ReaderFeature(
      GeoUtils.worldToLatLonCoords(rectangle(0, Math.sqrt(1))),
      new HashMap<>(Map.<String, Object>of(
        "name", "waterway",
        "name:es", "waterway es",
        "natural", "water",
        "water", "pond"
      )),
      OSM_SOURCE,
      null,
      10
    )));
  }

  @Test
  public void testMarinePoint() {
    assertFeatures(11, List.of(), process(new ReaderFeature(
      newLineString(0, 0, 1, 1),
      new HashMap<>(Map.<String, Object>of(
        "scalerank", 1,
        "name", "Black sea"
      )),
      NATURAL_EARTH_SOURCE,
      "ne_10m_geography_marine_polys",
      0
    )));

    // name match - use scale rank from NE
    assertFeatures(10, List.of(Map.of(
      "name", "Black Sea",
      "name:es", "Mar Negro",
      "_layer", "water_name",
      "_type", "point",
      "_minzoom", 1,
      "_maxzoom", 14
    )), process(pointFeature(Map.of(
      "rank", 9,
      "name", "Black Sea",
      "name:es", "Mar Negro",
      "place", "sea"
    ))));

    // name match but ocean - use min zoom=0
    assertFeatures(10, List.of(Map.of(
      "_layer", "water_name",
      "_type", "point",
      "_minzoom", 0,
      "_maxzoom", 14
    )), process(pointFeature(Map.of(
      "rank", 9,
      "name", "Black Sea",
      "place", "ocean"
    ))));

    // no name match - use OSM rank
    assertFeatures(10, List.of(Map.of(
      "_layer", "water_name",
      "_type", "point",
      "_minzoom", 9,
      "_maxzoom", 14
    )), process(pointFeature(Map.of(
      "rank", 9,
      "name", "Atlantic",
      "place", "sea"
    ))));

    // no rank at all, default to 8
    assertFeatures(10, List.of(Map.of(
      "_layer", "water_name",
      "_type", "point",
      "_minzoom", 8,
      "_maxzoom", 14
    )), process(pointFeature(Map.of(
      "name", "Atlantic",
      "place", "sea"
    ))));
  }
}
