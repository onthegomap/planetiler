package com.onthegomap.planetiler.examples;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.onthegomap.planetiler.TestUtils;
import com.onthegomap.planetiler.config.Arguments;
import com.onthegomap.planetiler.geo.GeoUtils;
import com.onthegomap.planetiler.mbtiles.Mbtiles;
import com.onthegomap.planetiler.reader.SimpleFeature;
import java.nio.file.Path;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.locationtech.jts.geom.Point;


class WaterInfrastructureProjectTest {
  private final WaterInfrastructureProject profile = new WaterInfrastructureProject();


  @Test
  public void testWaterWellMapping() {

    //1. create a fake OSM well
    var input = SimpleFeature.create(
      TestUtils.newPoint(0, 0), // Add geometry
      Map.of("man_made", "water_well", "name", "Juba Central Well")
    );


    //2. Execute
    var collector = TestUtils.processSourceFeature(input, profile);

    // 3. Verify: Should be in 'water_supply' layer with the right name
    assertEquals(1, collector.size());
    var feature = collector.getFirst();
    assertEquals("water_supply", feature.getLayer());
    assertEquals("Juba Central Well", feature.getAttrsAtZoom(14).get("name"));

  }

  @Test
  public void testIgnoreNonWaterAmenities() {
    // Setup: Create a school (which we decided to exclude)
    var input = SimpleFeature.create(
      TestUtils.newPoint(0, 0),
      Map.of("amenity", "school")
    );

    var collector = TestUtils.processSourceFeature(input, profile);

    // Verify: The 'water_supply' layer should be empty
    assertEquals(0, collector.size());
  }

  @Test
  void integrationTest(@TempDir Path tmpDir) throws Exception {
    Path dbPath = tmpDir.resolve("output.mbtiles");
    WaterInfrastructureProject.run(Arguments.of(
      // Override input source locations
      "osm_path", TestUtils.pathToResource("monaco-latest.osm.pbf"),
      // Override temp dir location
      "tmpdir", tmpDir.toString(),
      // Override output location
      "output", dbPath.toString()
    ));
    try (Mbtiles mbtiles = Mbtiles.newReadOnlyDatabase(dbPath)) {
      Map<String, String> metadata = mbtiles.metadataTable().getAll();
      assertEquals("Water Infrastructure Map", metadata.get("name"));
      TestUtils.assertContains("openstreetmap.org/copyright", metadata.get("attribution"));

      // Check that we have some water supply features (at least 1)
      TestUtils.assertNumFeatures(mbtiles, "water_supply", 14, Map.of(), GeoUtils.WORLD_LAT_LON_BOUNDS, 16,
        Point.class);

      TestUtils.assertTileDuplicates(mbtiles, 0);
    }
  }

}
