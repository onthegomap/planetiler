package com.onthegomap.planetiler.custommap;

import static com.onthegomap.planetiler.TestUtils.assertContains;
import static com.onthegomap.planetiler.TestUtils.assertFeatureNear;
import static com.onthegomap.planetiler.custommap.util.VerifyMonaco.MONACO_BOUNDS;
import static com.onthegomap.planetiler.util.Gzip.gunzip;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.DynamicTest.dynamicTest;

import com.onthegomap.planetiler.TestUtils;
import com.onthegomap.planetiler.VectorTile;
import com.onthegomap.planetiler.custommap.util.TestConfigurableUtils;
import com.onthegomap.planetiler.custommap.util.VerifyMonaco;
import com.onthegomap.planetiler.mbtiles.Mbtiles;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.io.TempDir;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.LineString;

/**
 * End-to-end tests for custommap generation.
 * <p>
 * Generates an entire map for the smallest openstreetmap extract available (Monaco) and asserts that expected output
 * features exist
 */
class ConfiguredMapTest {

  @TempDir
  static Path tmpDir;
  private static Mbtiles mbtiles;

  //  @BeforeAll
  public static void runPlanetiler() throws Exception {
    Path dbPath = tmpDir.resolve("output.mbtiles");
    ConfiguredMapMain.main(
      "generate-custom",
      "--schema=" + TestConfigurableUtils.pathToSample("owg_simple.yml"),
      "--download",

      // Override temp dir location
      "--tmp", tmpDir.toString(),

      // Override output location
      "--mbtiles=" + dbPath.toString()
    );
    System.err.println(dbPath.toFile());
    mbtiles = Mbtiles.newReadOnlyDatabase(dbPath);
  }

  //  @AfterAll
  public static void close() throws IOException {
    mbtiles.close();
  }

  //  @Test
  void testMetadata() {
    Map<String, String> metadata = mbtiles.metadata().getAll();
    assertEquals("OWG Simple Schema", metadata.get("name"));
    assertEquals("0", metadata.get("minzoom"));
    assertEquals("14", metadata.get("maxzoom"));
    assertEquals("baselayer", metadata.get("type"));
    assertEquals("pbf", metadata.get("format"));
    assertEquals("7.40921,43.72335,7.44864,43.75169", metadata.get("bounds"));
    assertEquals("7.42892,43.73752,14", metadata.get("center"));
    assertContains("Simple", metadata.get("description"));
    assertContains("www.openstreetmap.org/copyright", metadata.get("attribution"));
  }

  //  @Test
  void ensureValidGeometries() throws Exception {
    Set<Mbtiles.TileEntry> parsedTiles = TestUtils.getAllTiles(mbtiles);
    for (var tileEntry : parsedTiles) {
      var decoded = VectorTile.decode(gunzip(tileEntry.bytes()));
      for (VectorTile.Feature feature : decoded) {
        TestUtils.validateGeometry(feature.geometry().decode());
      }
    }
  }

  //  @Test
  void testContainsOceanPolyons() {
    assertFeatureNear(mbtiles, "water", Map.of(
      "natural", "water"
    ), 7.4484, 43.70783, 0, 14);
  }

  //  @Test
  void testRoad() {
    assertNumFeatures("road", Map.of(
      "highway", "primary"
    ), 14, 170, LineString.class);
  }

  //  @TestFactory
  Stream<DynamicTest> testVerifyChecks() {
    return VerifyMonaco.verify(mbtiles).results().stream()
      .map(check -> dynamicTest(check.name(), () -> {
        check.error().ifPresent(Assertions::fail);
      }));
  }

  private static void assertNumFeatures(String layer, Map<String, Object> attrs, int zoom,
    int expected, Class<? extends Geometry> clazz) {
    TestUtils.assertNumFeatures(mbtiles, layer, zoom, attrs, MONACO_BOUNDS, expected, clazz);
  }
}
