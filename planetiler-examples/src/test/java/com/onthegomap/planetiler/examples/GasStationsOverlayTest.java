package com.onthegomap.planetiler.examples;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.onthegomap.planetiler.FeatureCollector;
import com.onthegomap.planetiler.TestUtils;
import com.onthegomap.planetiler.config.Arguments;
import com.onthegomap.planetiler.geo.GeoUtils;
import com.onthegomap.planetiler.mbtiles.Mbtiles;
import com.onthegomap.planetiler.reader.SimpleFeature;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.locationtech.jts.geom.Point;

class GasStationsOverlayTest {


  private final GasStationsOverlay profile = new GasStationsOverlay();

  /**
   * a test for basic gas station feature creation
   * ive taken luxembourg as an example so the coords and gasstation nrand name etc are 
   * in respect to that.
   */
  @Test
  void testGasStationProcessing() {

    var feature = SimpleFeature.createFakeOsmFeature(
      TestUtils.newPoint(6.13, 49.61), 
      Map.of(
        "amenity", "fuel",
        "brand", "Aral",
        "name", "Aral Station",
        "operator", "Aral Luxembourg",
        "hgv", "yes",
        "opening_hours", "24/7"
      ),
      null,
      null,
      1,
      List.of()
    );

    List<FeatureCollector.Feature> results =
      TestUtils.processSourceFeature(feature, profile);

    assertEquals(1, results.size());

    var result = results.getFirst();

    assertEquals("gas_station", result.getLayer());
    assertEquals("Aral Station", result.getAttrsAtZoom(12).get("name"));
    assertEquals("Aral", result.getAttrsAtZoom(12).get("brand"));
    assertEquals("Aral Luxembourg", result.getAttrsAtZoom(12).get("operator"));
    assertEquals("yes", result.getAttrsAtZoom(12).get("hgv"));
    assertEquals("24/7", result.getAttrsAtZoom(12).get("opening_hours"));
    assertEquals(12, result.getMinZoom());
  }

  /**
   * Test fallback: use brand when name is missing
   */
  @Test
  void testBrandFallback() {

    var feature = SimpleFeature.createFakeOsmFeature(
      TestUtils.newPoint(6.10, 49.60),
      Map.of(
        "amenity", "fuel",
        "brand", "Total"
      ),
      null,
      null,
      2,
      List.of()
    );

    List<FeatureCollector.Feature> results =
      TestUtils.processSourceFeature(feature, profile);

    assertEquals(1, results.size());

    var result = results.getFirst();

    assertEquals("Total", result.getAttrsAtZoom(12).get("name"));
  }

  /**
   * Test that non-fuel features are ignored
   */
  @Test
  void testIgnoreNonFuel() {

    var feature = SimpleFeature.createFakeOsmFeature(
      TestUtils.newPoint(6.0, 49.5),
      Map.of("amenity", "cafe"),
      null,
      null,
      3,
      List.of()
    );

    List<FeatureCollector.Feature> results =
      TestUtils.processSourceFeature(feature, profile);

    assertEquals(0, results.size());
  }
}