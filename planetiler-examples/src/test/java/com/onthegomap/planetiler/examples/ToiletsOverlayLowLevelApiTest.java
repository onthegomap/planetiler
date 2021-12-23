package com.onthegomap.planetiler.examples;

import static com.onthegomap.planetiler.TestUtils.assertContains;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.onthegomap.planetiler.TestUtils;
import com.onthegomap.planetiler.geo.GeoUtils;
import com.onthegomap.planetiler.mbtiles.Mbtiles;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.locationtech.jts.geom.Point;

public class ToiletsOverlayLowLevelApiTest {

  @Test
  public void integrationTest(@TempDir Path tmpDir) throws IOException {
    Path dbPath = tmpDir.resolve("output.mbtiles");
    ToiletsOverlayLowLevelApi.run(
      // Override input source locations
      TestUtils.pathToResource("monaco-latest.osm.pbf"),
      // Override temp dir location
      tmpDir,
      // Override output location
      dbPath
    );
    try (Mbtiles mbtiles = Mbtiles.newReadOnlyDatabase(dbPath)) {
      Map<String, String> metadata = mbtiles.metadata().getAll();
      assertEquals("Toilets Overlay", metadata.get("name"));
      assertContains("openstreetmap.org/copyright", metadata.get("attribution"));

      TestUtils.assertNumFeatures(mbtiles, "toilets", 14, Map.of(), GeoUtils.WORLD_LAT_LON_BOUNDS,
        34, Point.class);
    }
  }
}
