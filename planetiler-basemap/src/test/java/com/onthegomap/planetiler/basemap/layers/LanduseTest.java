package com.onthegomap.planetiler.basemap.layers;

import static com.onthegomap.planetiler.TestUtils.rectangle;
import static com.onthegomap.planetiler.basemap.BasemapProfile.NATURAL_EARTH_SOURCE;

import com.onthegomap.planetiler.geo.GeoUtils;
import com.onthegomap.planetiler.reader.SimpleFeature;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

public class LanduseTest extends AbstractLayerTest {

  @Test
  public void testNaturalEarthUrbanAreas() {
    assertFeatures(
        0,
        List.of(
            Map.of(
                "_layer", "landuse",
                "class", "residential",
                "_buffer", 4d)),
        process(
            SimpleFeature.create(
                GeoUtils.worldToLatLonCoords(rectangle(0, Math.sqrt(1))),
                Map.of("scalerank", 1.9),
                NATURAL_EARTH_SOURCE,
                "ne_50m_urban_areas",
                0)));
    assertFeatures(
        0,
        List.of(),
        process(
            SimpleFeature.create(
                GeoUtils.worldToLatLonCoords(rectangle(0, Math.sqrt(1))),
                Map.of("scalerank", 2.1),
                NATURAL_EARTH_SOURCE,
                "ne_50m_urban_areas",
                0)));
  }

  @Test
  public void testOsmLanduse() {
    assertFeatures(
        13,
        List.of(
            Map.of("_layer", "poi"),
            Map.of(
                "_layer",
                "landuse",
                "class",
                "railway",
                "_minpixelsize",
                4d,
                "_minzoom",
                9,
                "_maxzoom",
                14)),
        process(
            polygonFeature(
                Map.of(
                    "landuse", "railway",
                    "amenity", "school"))));
    assertFeatures(
        13,
        List.of(
            Map.of("_layer", "poi"),
            Map.of(
                "_layer",
                "landuse",
                "class",
                "school",
                "_minpixelsize",
                4d,
                "_minzoom",
                9,
                "_maxzoom",
                14)),
        process(polygonFeature(Map.of("amenity", "school"))));
  }

  @Test
  public void testGraveYardBecomesCemetery() {
    assertFeatures(
        14,
        List.of(
            Map.of("_layer", "poi"),
            Map.of(
                "_layer", "landuse",
                "class", "cemetery")),
        process(polygonFeature(Map.of("amenity", "grave_yard"))));
  }

  @Test
  public void testOsmLanduseLowerZoom() {
    assertFeatures(
        6,
        List.of(
            Map.of(
                "_layer",
                "landuse",
                "class",
                "suburb",
                "_minzoom",
                6,
                "_maxzoom",
                14,
                "_minpixelsize",
                1d)),
        process(polygonFeature(Map.of("place", "suburb"))));
    assertFeatures(
        7,
        List.of(
            Map.of(
                "_layer",
                "landuse",
                "class",
                "residential",
                "_minzoom",
                6,
                "_maxzoom",
                14,
                "_minpixelsize",
                2d)),
        process(polygonFeature(Map.of("landuse", "residential"))));
  }
}
