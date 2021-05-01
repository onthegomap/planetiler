package com.onthegomap.flatmap.reader;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.onthegomap.flatmap.Profile;
import com.onthegomap.flatmap.geo.GeoUtils;
import com.onthegomap.flatmap.monitoring.Stats;
import com.onthegomap.flatmap.worker.Topology;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.locationtech.jts.geom.Geometry;

public class ShapefileReaderTest {

  private ShapefileReader reader = new ShapefileReader(
    Path.of("src", "test", "resources", "shapefile.zip"),
    new Profile.NullProfile(),
    new Stats.InMemory()
  );

  @AfterEach
  public void close() {
    reader.close();
  }

  @Test
  public void testCount() {
    assertEquals(86, reader.getCount());
    assertEquals(86, reader.getCount());
  }

  @Test
  @Timeout(30)
  public void testReadShapefile() {
    for (int i = 1; i <= 2; i++) {
      List<Geometry> points = new ArrayList<>();
      Topology.start("test", new Stats.InMemory())
        .fromGenerator("shapefile", reader.read())
        .addBuffer("reader_queue", 100, 1)
        .sinkToConsumer("counter", 1, elem -> {
          assertTrue(elem.getTag("name") instanceof String);
          points.add(elem.geometry());
        }).await();
      assertEquals(86, points.size());
      var gc = GeoUtils.gf.createGeometryCollection(points.toArray(new Geometry[0]));
      var centroid = gc.getCentroid();
      assertEquals(-77.0297995, centroid.getX(), 5, "iter " + i);
      assertEquals(38.9119684, centroid.getY(), 5, "iter " + i);
    }
  }
}
