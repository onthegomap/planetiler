package com.onthegomap.planetiler.reader;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.onthegomap.planetiler.Profile;
import com.onthegomap.planetiler.TestUtils;
import com.onthegomap.planetiler.collection.FeatureGroup;
import com.onthegomap.planetiler.config.PlanetilerConfig;
import com.onthegomap.planetiler.geo.GeoUtils;
import com.onthegomap.planetiler.stats.Stats;
import com.onthegomap.planetiler.worker.WorkerPipeline;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.locationtech.jts.geom.Geometry;

class ShapefileReaderTest {
  private final Path shapefilePath = TestUtils.pathToResource("shapefile.zip");

  private final ShapefileReader reader = new ShapefileReader(
    "test",
    shapefilePath,
    new Profile.NullProfile(),
    Stats.inMemory()
  );

  @AfterEach
  public void close() {
    reader.close();
  }

  @Test
  void testCount() {
    assertEquals(86, reader.getCount());
    assertEquals(86, reader.getCount());
  }

  @Test
  @Timeout(30)
  void testReadShapefile() {
    for (int i = 1; i <= 2; i++) {
      String name = "iter " + i;
      var theReader = reader;
      List<Geometry> points = new ArrayList<>();
      List<String> names = new ArrayList<>();
      WorkerPipeline.start("test", Stats.inMemory())
        .fromGenerator("shapefile", theReader.read())
        .addBuffer("reader_queue", 100, 1)
        .sinkToConsumer("counter", 1, elem -> {
          assertTrue(elem.getTag("name") instanceof String);
          assertEquals("test", elem.getSource());
          assertNull(elem.getSourceLayer());
          points.add(elem.latLonGeometry());
          names.add(elem.getTag("name").toString());
        }).await();
      assertEquals(86, points.size());
      assertTrue(names.contains("Van Dörn Street"));
      var gc = GeoUtils.JTS_FACTORY.createGeometryCollection(points.toArray(new Geometry[0]));
      var centroid = gc.getCentroid();
      assertEquals(-77.0297995, centroid.getX(), 5, name);
      assertEquals(38.9119684, centroid.getY(), 5, name);
    }
  }

  @Test
  @Timeout(30)
  void testCopyAndReadShapefile(@TempDir Path tempDir) throws IOException {
    var tempPath = tempDir.resolve("tmpshapefile.zip");
    var profile = new Profile.NullProfile();
    var config = PlanetilerConfig.defaults();
    var stats = Stats.inMemory();
    Files.copy(shapefilePath, tempPath);
    var featureGroup = FeatureGroup.newInMemoryFeatureGroup(profile, stats);
    ShapefileReader.process("test", tempPath, featureGroup, config, profile, stats);
  }
}
