package com.onthegomap.planetiler.examples;

import static com.onthegomap.planetiler.TestUtils.assertContains;
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
import org.locationtech.jts.geom.Polygon;

class ParksOverlayTest {

  private final ParksOverlay profile = new ParksOverlay();

  @Test
  void testParkPolygonEmitted() {
    var polygon = SimpleFeature.create(
      TestUtils.newPolygon(0, 0, 1, 0, 1, 1, 0, 1, 0, 0),
      Map.of("leisure", "park", "name", "Jardin Exotique")
    );
    List<FeatureCollector.Feature> mapFeatures = TestUtils.processSourceFeature(polygon, profile);

    assertEquals(1, mapFeatures.size());
    var feature = mapFeatures.getFirst();
    assertEquals("parks", feature.getLayer());
    assertEquals(Map.of(
      "name", "Jardin Exotique",
      "leisure", "park"
    ), feature.getAttrsAtZoom(14));
    assertEquals(4, feature.getMinZoom());
    assertEquals(14, feature.getMaxZoom());
  }

  @Test
  void testGardenPolygonEmitted() {
    var polygon = SimpleFeature.create(
      TestUtils.newPolygon(0, 0, 1, 0, 1, 1, 0, 1, 0, 0),
      Map.of("leisure", "garden", "name", "Rose Garden", "access", "yes")
    );
    List<FeatureCollector.Feature> mapFeatures = TestUtils.processSourceFeature(polygon, profile);

    assertEquals(1, mapFeatures.size());
    var feature = mapFeatures.getFirst();
    assertEquals("parks", feature.getLayer());
    assertEquals(Map.of(
      "name", "Rose Garden",
      "leisure", "garden",
      "access", "yes"
    ), feature.getAttrsAtZoom(14));
  }

  @Test
  void testNonParkLeisureIgnored() {
    var polygon = SimpleFeature.create(
      TestUtils.newPolygon(0, 0, 1, 0, 1, 1, 0, 1, 0, 0),
      Map.of("leisure", "pitch")
    );
    List<FeatureCollector.Feature> mapFeatures = TestUtils.processSourceFeature(polygon, profile);
    assertEquals(0, mapFeatures.size());
  }

  @Test
  void testPointNodeIgnored() {
    // Parks are polygons; a node tagged leisure=park should not produce output
    var node = SimpleFeature.create(
      TestUtils.newPoint(1, 2),
      Map.of("leisure", "park", "name", "Tiny Park")
    );
    List<FeatureCollector.Feature> mapFeatures = TestUtils.processSourceFeature(node, profile);
    assertEquals(0, mapFeatures.size());
  }

  @Test
  void integrationTest(@TempDir Path tmpDir) throws Exception {
    Path dbPath = tmpDir.resolve("output.mbtiles");
    ParksOverlay.run(Arguments.of(
      // Override input source locations
      "osm_path", TestUtils.pathToResource("monaco-latest.osm.pbf"),
      // Override temp dir location
      "tmpdir", tmpDir.toString(),
      // Override output location
      "output", dbPath.toString()
    ));
    try (Mbtiles mbtiles = Mbtiles.newReadOnlyDatabase(dbPath)) {
      Map<String, String> metadata = mbtiles.metadata().toMap();
      assertEquals("Parks Overlay", metadata.get("name"));
      assertContains("openstreetmap.org/copyright", metadata.get("attribution"));

      TestUtils.assertNumFeatures(mbtiles, "parks", 14, Map.of(), GeoUtils.WORLD_LAT_LON_BOUNDS, 53, Polygon.class);
      TestUtils.assertTileDuplicates(mbtiles, 0);
    }
  }
}
