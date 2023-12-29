package com.onthegomap.planetiler.experimental.lua;

import static com.onthegomap.planetiler.TestUtils.assertContains;
import static com.onthegomap.planetiler.util.Gzip.gunzip;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.onthegomap.planetiler.TestUtils;
import com.onthegomap.planetiler.VectorTile;
import com.onthegomap.planetiler.mbtiles.Mbtiles;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.io.TempDir;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.LineString;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
abstract class PlanetilerLuaTest {

  private final String script;

  PlanetilerLuaTest(String script) {
    this.script = script;
  }

  static class WithMainTest extends PlanetilerLuaTest {
    WithMainTest() {
      super("roads_main.lua");
    }
  }
  static class WithoutMainTest extends PlanetilerLuaTest {
    WithoutMainTest() {
      super("roads.lua");
    }
  }

  public static final Envelope MONACO_BOUNDS = new Envelope(7.40921, 7.44864, 43.72335, 43.75169);

  @TempDir
  static Path tmpDir;
  private Mbtiles mbtiles;

  @BeforeAll
  void runPlanetiler() throws Exception {
    Path dbPath = tmpDir.resolve("output.mbtiles");
    LuaMain.main(
      // Use local data extracts instead of downloading
      "--script=" + pathToResource(script),
      "--osm_path=" + TestUtils.pathToResource("monaco-latest.osm.pbf"),

      // Override temp dir location
      "--tmp=" + tmpDir,

      // Override output location
      "--output=" + dbPath
    );
    mbtiles = Mbtiles.newReadOnlyDatabase(dbPath);
  }

  @AfterAll
  public void close() throws IOException {
    mbtiles.close();
  }

  @Test
  void testMetadata() {
    Map<String, String> metadata = mbtiles.metadataTable().getAll();
    assertEquals("Road Schema", metadata.get("name"));
    assertEquals("0", metadata.get("minzoom"));
    assertEquals("14", metadata.get("maxzoom"));
    assertEquals("baselayer", metadata.get("type"));
    assertEquals("pbf", metadata.get("format"));
    assertEquals("7.40921,43.72335,7.44864,43.75169", metadata.get("bounds"));
    assertEquals("7.42892,43.73752,14", metadata.get("center"));
    assertContains("Simple", metadata.get("description"));
    assertContains("www.openstreetmap.org/copyright", metadata.get("attribution"));
  }

  @Test
  void ensureValidGeometries() throws Exception {
    var parsedTiles = TestUtils.getTiles(mbtiles);
    for (var tileEntry : parsedTiles) {
      var decoded = VectorTile.decode(gunzip(tileEntry.bytes()));
      for (VectorTile.Feature feature : decoded) {
        TestUtils.validateGeometry(feature.geometry().decode());
      }
    }
  }

  @Test
  void testRoads() {
    assertMinFeatures("road", Map.of(
      "highway", "primary"
    ), 14, 317, LineString.class);
    assertMinFeatures("road", Map.of(
      "highway", "service"
    ), 14, 310, LineString.class);
  }

  private void assertMinFeatures(String layer, Map<String, Object> attrs, int zoom,
    int expected, Class<? extends Geometry> clazz) {
    TestUtils.assertMinFeatureCount(mbtiles, layer, zoom, attrs, MONACO_BOUNDS, expected, clazz);
  }

  public static Path pathToResource(String resource) {
    return resolve(Path.of("planetiler-experimental", "src", "test", "resources", resource));
  }


  private static Path resolve(Path pathFromRoot) {
    Path cwd = Path.of("").toAbsolutePath();
    return cwd.resolveSibling(pathFromRoot);
  }
}
