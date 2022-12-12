package com.onthegomap.planetiler.reader;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.onthegomap.planetiler.Profile;
import com.onthegomap.planetiler.TestUtils;
import com.onthegomap.planetiler.geo.GeoUtils;
import com.onthegomap.planetiler.stats.Stats;
import com.onthegomap.planetiler.worker.WorkerPipeline;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.locationtech.jts.geom.Geometry;

class GeoPackageReaderTest {

  @Test
  @Timeout(30)
  void testReadGeoPackage() {
    testReadGeoPackage(TestUtils.pathToResource("geopackage.gpkg"));
  }

  private static void testReadGeoPackage(Path path) {
    try (
      var reader = new GeoPackageReader(
        "test",
        path,
        new Profile.NullProfile(),
        Stats.inMemory()
      )
    ) {
      for (int i = 1; i <= 2; i++) {
        assertEquals(86, reader.getCount());
        List<Geometry> points = new ArrayList<>();
        List<String> names = new ArrayList<>();
        WorkerPipeline.start("test", Stats.inMemory())
          .fromGenerator("geopackage", reader.read())
          .addBuffer("reader_queue", 100, 1)
          .sinkToConsumer("counter", 1, elem -> {
            assertTrue(elem.getTag("name") instanceof String);
            assertEquals("test", elem.getSource());
            assertEquals("stations", elem.getSourceLayer());
            points.add(elem.latLonGeometry());
            names.add(elem.getTag("name").toString());
          }).await();
        assertEquals(86, points.size());
        assertTrue(names.contains("Van Dörn Street"));
        var gc = GeoUtils.JTS_FACTORY.createGeometryCollection(points.toArray(new Geometry[0]));
        var centroid = gc.getCentroid();
        assertEquals(-77.0297995, centroid.getX(), 5, "iter " + i);
        assertEquals(38.9119684, centroid.getY(), 5, "iter " + i);
      }
    }
  }
}
