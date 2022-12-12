package com.onthegomap.planetiler.reader;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
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
import java.util.Objects;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.locationtech.jts.geom.Geometry;

class NaturalEarthReaderTest {

  @ParameterizedTest
  @ValueSource(strings = {"natural_earth_vector.sqlite", "natural_earth_vector.sqlite.zip"})
  @Timeout(30)
  void testReadNaturalEarth(String filename, @TempDir Path tempDir) {
    var path = TestUtils.pathToResource(filename);
    try (var reader = new NaturalEarthReader("test", path, tempDir, new Profile.NullProfile(), Stats.inMemory())) {
      for (int i = 1; i <= 2; i++) {
        assertEquals(7_679, reader.getCount(), "iter " + i);

        List<Geometry> points = new ArrayList<>();
        WorkerPipeline.start("test", Stats.inMemory())
          .readFromTiny("source_paths", reader.sourcePaths)
          .addWorker("naturalearth", 1, reader.read())
          .addBuffer("reader_queue", 100, 1)
          .sinkToConsumer("counter", 1, elem -> {
            Object elevation = elem.getTag("elevation");
            if ("ne_110m_geography_regions_elevation_points".equals(elem.getSourceLayer())) {
              assertTrue(elevation instanceof Double, Objects.toString(elevation));
              assertEquals("test", elem.getSource());
              points.add(elem.latLonGeometry());
            }
          }).await();
        assertEquals(19, points.size());
        var gc = GeoUtils.JTS_FACTORY.createGeometryCollection(points.toArray(new Geometry[0]));
        var centroid = gc.getCentroid();
        assertArrayEquals(
          new double[]{14.22422, 12.994629},
          new double[]{centroid.getX(), centroid.getY()}, 5,
          "iter " + i
        );
      }
    }
  }
}
