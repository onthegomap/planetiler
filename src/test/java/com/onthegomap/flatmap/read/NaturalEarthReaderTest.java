package com.onthegomap.flatmap.read;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.onthegomap.flatmap.Profile;
import com.onthegomap.flatmap.geo.GeoUtils;
import com.onthegomap.flatmap.monitoring.Stats;
import com.onthegomap.flatmap.worker.Topology;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.locationtech.jts.geom.Geometry;

public class NaturalEarthReaderTest {

  @ParameterizedTest
  @ValueSource(strings = {"natural_earth_vector.sqlite", "natural_earth_vector.sqlite.zip"})
  @Timeout(30)
  public void testReadNaturalEarth(String filename, @TempDir Path tempDir) {
    var path = Path.of("src", "test", "resources", filename);
    try (var reader = new NaturalEarthReader("test", path, tempDir, new Profile.NullProfile(), new Stats.InMemory())) {
      for (int i = 1; i <= 2; i++) {
        assertEquals(19, reader.getCount(), "iter " + i);

        List<Geometry> points = new ArrayList<>();
        Topology.start("test", new Stats.InMemory())
          .fromGenerator("naturalearth", reader.read())
          .addBuffer("reader_queue", 100, 1)
          .sinkToConsumer("counter", 1, elem -> {
            Object elevation = elem.getTag("elevation");
            assertTrue(elevation instanceof Double, Objects.toString(elevation));
            assertEquals("test", elem.getSource());
            assertEquals("ne_110m_geography_regions_elevation_points", elem.getSourceLayer());
            points.add(elem.latLonGeometry());
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
