package com.onthegomap.planetiler.reader;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.onthegomap.planetiler.TestUtils;
import com.onthegomap.planetiler.geo.GeoUtils;
import com.onthegomap.planetiler.stats.Stats;
import com.onthegomap.planetiler.util.FileUtils;
import com.onthegomap.planetiler.worker.WorkerPipeline;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;

class GeoPackageReaderTest {
  @TempDir
  static Path tmpDir;

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  @Timeout(30)
  void testReadGeoPackage(boolean keepUnzipped) throws IOException {
    Path pathOutsideZip = TestUtils.pathToResource("geopackage.gpkg");
    Path zipPath = TestUtils.pathToResource("geopackage.gpkg.zip");
    Path pathInZip = FileUtils.walkPathWithPattern(zipPath, "*.gpkg").get(0);

    var projections = new String[]{null, "EPSG:4326"};

    for (var path : List.of(pathOutsideZip, pathInZip)) {
      for (var proj : projections) {
        try (
          var reader = new GeoPackageReader(proj, "test", path, tmpDir, keepUnzipped, null)
        ) {
          for (int iter = 0; iter < 2; iter++) {
            String id = "path=" + path + " proj=" + proj + " iter=" + iter;
            assertEquals(86, reader.getFeatureCount(), id);
            List<Geometry> points = new ArrayList<>();
            List<String> names = new ArrayList<>();
            WorkerPipeline.start("test", Stats.inMemory())
              .fromGenerator("geopackage", reader::readFeatures, 1)
              .addBuffer("reader_queue", 100, 1)
              .sinkToConsumer("counter", 1, elem -> {
                assertTrue(elem.getTag("name") instanceof String);
                assertEquals("test", elem.getSource());
                assertEquals("stations", elem.getSourceLayer());
                points.add(elem.latLonGeometry());
                names.add(elem.getTag("name").toString());
              }).await();
            assertEquals(86, points.size(), id);
            assertTrue(names.contains("Van Dörn Street"), id);
            var gc = GeoUtils.JTS_FACTORY.createGeometryCollection(points.toArray(new Geometry[0]));
            var centroid = gc.getCentroid();
            assertEquals(-77.0297995, centroid.getX(), 5, id);
            assertEquals(38.9119684, centroid.getY(), 5, id);
          }
        }
      }
    }
  }


  @Test
  @Timeout(30)
  void testReadGeoPackageSpatialIndex() throws IOException {
    Path pathOutsideZip = TestUtils.pathToResource("geopackage.gpkg");
    Path zipPath = TestUtils.pathToResource("geopackage.gpkg.zip");
    Path pathInZip = FileUtils.walkPathWithPattern(zipPath, "*.gpkg").get(0);

    var projections = new String[]{null, "EPSG:4326"};

    for (var path : List.of(pathOutsideZip, pathInZip)) {
      for (var proj : projections) {
        try (
          var reader =
            new GeoPackageReader(proj, "test", path, tmpDir, false, new Envelope(-77.0306, -77.0192, 38.8894, 38.9014))
        ) {
          for (int iter = 0; iter < 2; iter++) {
            String id = "path=" + path + " proj=" + proj + " iter=" + iter;
            assertEquals(86, reader.getFeatureCount(), id);
            List<Geometry> points = new ArrayList<>();
            List<String> names = new ArrayList<>();
            WorkerPipeline.start("test", Stats.inMemory())
              .fromGenerator("geopackage", reader::readFeatures, 1)
              .addBuffer("reader_queue", 100, 1)
              .sinkToConsumer("counter", 1, elem -> {
                points.add(elem.latLonGeometry());
              }).await();
            assertEquals(4, points.size(), id);
          }
        }
      }
    }
  }

  @Test
  @Timeout(30)
  void testReadEmptyGeoPackage() throws IOException {
    Path path = TestUtils.pathToResource("empty-geom.gpkg");

    try (
      var reader = new GeoPackageReader(null, "test", path, tmpDir, false, null)
    ) {
      for (int iter = 0; iter < 2; iter++) {
        String id = "iter=" + iter;
        assertEquals(1, reader.getFeatureCount(), id);
        AtomicInteger found = new AtomicInteger(0);
        WorkerPipeline.start("test", Stats.inMemory())
          .fromGenerator("geopackage", reader::readFeatures, 1)
          .addBuffer("reader_queue", 100, 1)
          .sinkToConsumer("counter", 1, elem -> {
            found.incrementAndGet();
          }).await();
        assertEquals(0, found.get());
      }
    }
  }
}
