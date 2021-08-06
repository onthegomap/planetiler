package com.onthegomap.flatmap.openmaptiles.layers;

import static com.onthegomap.flatmap.TestUtils.rectangle;
import static com.onthegomap.flatmap.openmaptiles.OpenMapTilesProfile.NATURAL_EARTH_SOURCE;
import static com.onthegomap.flatmap.openmaptiles.OpenMapTilesProfile.OSM_SOURCE;
import static com.onthegomap.flatmap.openmaptiles.OpenMapTilesProfile.WATER_POLYGON_SOURCE;

import com.onthegomap.flatmap.reader.ReaderFeature;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

public class WaterTest extends AbstractLayerTest {


  @Test
  public void testWaterNaturalEarth() {
    assertFeatures(0, List.of(Map.of(
      "class", "lake",
      "intermittent", "<null>",
      "_layer", "water",
      "_type", "polygon",
      "_minzoom", 0
    )), process(new ReaderFeature(
      rectangle(0, 10),
      Map.of(),
      NATURAL_EARTH_SOURCE,
      "ne_110m_lakes",
      0
    )));

    assertFeatures(0, List.of(Map.of(
      "class", "ocean",
      "intermittent", "<null>",
      "_layer", "water",
      "_type", "polygon",
      "_minzoom", 0
    )), process(new ReaderFeature(
      rectangle(0, 10),
      Map.of(),
      NATURAL_EARTH_SOURCE,
      "ne_110m_ocean",
      0
    )));

    assertFeatures(6, List.of(Map.of(
      "class", "lake",
      "_layer", "water",
      "_type", "polygon",
      "_maxzoom", 5
    )), process(new ReaderFeature(
      rectangle(0, 10),
      Map.of(),
      NATURAL_EARTH_SOURCE,
      "ne_10m_lakes",
      0
    )));

    assertFeatures(6, List.of(Map.of(
      "class", "ocean",
      "_layer", "water",
      "_type", "polygon",
      "_maxzoom", 5
    )), process(new ReaderFeature(
      rectangle(0, 10),
      Map.of(),
      NATURAL_EARTH_SOURCE,
      "ne_10m_ocean",
      0
    )));
  }

  @Test
  public void testWaterOsmWaterPolygon() {
    assertFeatures(0, List.of(Map.of(
      "class", "ocean",
      "intermittent", "<null>",
      "_layer", "water",
      "_type", "polygon",
      "_minzoom", 6,
      "_maxzoom", 14
    )), process(new ReaderFeature(
      rectangle(0, 10),
      Map.of(),
      WATER_POLYGON_SOURCE,
      null,
      0
    )));
  }

  @Test
  public void testWater() {
    assertFeatures(14, List.of(Map.of(
      "class", "lake",
      "_layer", "water",
      "_type", "polygon",
      "_minzoom", 6,
      "_maxzoom", 14
    )), process(polygonFeature(Map.of(
      "natural", "water",
      "water", "reservoir"
    ))));
    assertFeatures(14, List.of(
      Map.of("_layer", "poi"),
      Map.of(
        "class", "lake",

        "_layer", "water",
        "_type", "polygon",
        "_minzoom", 6,
        "_maxzoom", 14
      )), process(polygonFeature(Map.of(
      "leisure", "swimming_pool"
    ))));
    assertFeatures(14, List.of(), process(polygonFeature(Map.of(
      "natural", "bay"
    ))));
    assertFeatures(14, List.of(Map.of()), process(polygonFeature(Map.of(
      "natural", "water"
    ))));
    assertFeatures(14, List.of(), process(polygonFeature(Map.of(
      "natural", "water",
      "covered", "yes"
    ))));
    assertFeatures(14, List.of(Map.of(
      "class", "river",
      "brunnel", "bridge",
      "intermittent", 1,

      "_layer", "water",
      "_type", "polygon",
      "_minzoom", 6,
      "_maxzoom", 14
    )), process(polygonFeature(Map.of(
      "waterway", "stream",
      "bridge", "1",
      "intermittent", "1"
    ))));
    assertFeatures(11, List.of(Map.of(
      "class", "lake",
      "brunnel", "<null>",
      "intermittent", 0,

      "_layer", "water",
      "_type", "polygon",
      "_minzoom", 6,
      "_maxzoom", 14,
      "_minpixelsize", 2d
    )), process(polygonFeature(Map.of(
      "landuse", "salt_pond",
      "bridge", "1"
    ))));
  }

  @Test
  public void testOceanZoomLevels() {
    assertCoversZoomRange(0, 14, "water",
      process(new ReaderFeature(
        rectangle(0, 10),
        Map.of(),
        NATURAL_EARTH_SOURCE,
        "ne_110m_ocean",
        0
      )),
      process(new ReaderFeature(
        rectangle(0, 10),
        Map.of(),
        NATURAL_EARTH_SOURCE,
        "ne_50m_ocean",
        0
      )),
      process(new ReaderFeature(
        rectangle(0, 10),
        Map.of(),
        NATURAL_EARTH_SOURCE,
        "ne_10m_ocean",
        0
      )),
      process(new ReaderFeature(
        rectangle(0, 10),
        Map.of(),
        WATER_POLYGON_SOURCE,
        null,
        0
      ))
    );
  }

  @Test
  public void testLakeZoomLevels() {
    assertCoversZoomRange(0, 14, "water",
      process(new ReaderFeature(
        rectangle(0, 10),
        Map.of(),
        NATURAL_EARTH_SOURCE,
        "ne_110m_lakes",
        0
      )),
      process(new ReaderFeature(
        rectangle(0, 10),
        Map.of(),
        NATURAL_EARTH_SOURCE,
        "ne_50m_lakes",
        0
      )),
      process(new ReaderFeature(
        rectangle(0, 10),
        Map.of(),
        NATURAL_EARTH_SOURCE,
        "ne_10m_lakes",
        0
      )),
      process(new ReaderFeature(
        rectangle(0, 10),
        Map.of(
          "natural", "water",
          "water", "reservoir"
        ),
        OSM_SOURCE,
        null,
        0
      ))
    );
  }
}
