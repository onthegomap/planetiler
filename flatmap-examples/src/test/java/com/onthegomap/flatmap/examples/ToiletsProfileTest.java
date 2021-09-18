package com.onthegomap.flatmap.examples;

import static com.onthegomap.flatmap.TestUtils.assertContains;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.onthegomap.flatmap.FeatureCollector;
import com.onthegomap.flatmap.TestUtils;
import com.onthegomap.flatmap.config.Arguments;
import com.onthegomap.flatmap.geo.GeoUtils;
import com.onthegomap.flatmap.mbiles.Mbtiles;
import com.onthegomap.flatmap.reader.SimpleFeature;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.locationtech.jts.geom.Point;

public class ToiletsProfileTest {

  private final ToiletsOverlay profile = new ToiletsOverlay();

  @Test
  public void testSourceFeatureProcessing() {
    var node = SimpleFeature.create(
      TestUtils.newPoint(1, 2),
      Map.of("amenity", "toilets")
    );
    List<FeatureCollector.Feature> mapFeatures = TestUtils.processSourceFeature(node, profile);

    // verify output feature
    assertEquals(1, mapFeatures.size());
    var feature = mapFeatures.get(0);
    assertEquals("toilets", feature.getLayer());
    // no attributes
    assertEquals(Map.of(), feature.getAttrsAtZoom(14));
    assertEquals(0, feature.getMinZoom());
    assertEquals(14, feature.getMaxZoom());
    assertEquals(1, feature.getSortKey());

    // at z12 - use label grid to limit output
    assertEquals(4, feature.getPointLabelGridLimitAtZoom(12));
    assertEquals(32, feature.getPointLabelGridPixelSizeAtZoom(12));
    assertEquals(32, feature.getBufferPixelsAtZoom(12));

    // at z13 - no label grid (0 disables filtering)
    assertEquals(0, feature.getPointLabelGridLimitAtZoom(13));
    assertEquals(0, feature.getPointLabelGridPixelSizeAtZoom(13));
    assertEquals(4, feature.getBufferPixelsAtZoom(13));
  }

  @Test
  public void integrationTest(@TempDir Path tmpDir) throws Exception {
    Path dbPath = tmpDir.resolve("output.mbtiles");
    ToiletsOverlay.run(Arguments.of(
      // Override input source locations
      "osm_path", TestUtils.pathToResource("monaco-latest.osm.pbf"),
      // Override temp dir location
      "tmp", tmpDir.toString(),
      // Override output location
      "mbtiles", dbPath.toString()
    ));
    try (Mbtiles mbtiles = Mbtiles.newReadOnlyDatabase(dbPath)) {
      Map<String, String> metadata = mbtiles.metadata().getAll();
      assertEquals("Toilets Overlay", metadata.get("name"));
      assertContains("openstreetmap.org/copyright", metadata.get("attribution"));

      TestUtils.assertNumFeatures(mbtiles, "toilets", 14, Map.of(), GeoUtils.WORLD_LAT_LON_BOUNDS,
        34, Point.class);
    }
  }
}
