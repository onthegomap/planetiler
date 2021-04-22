package com.onthegomap.flatmap.reader;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.onthegomap.flatmap.GeoUtils;
import com.onthegomap.flatmap.monitoring.Stats.InMemory;
import com.onthegomap.flatmap.worker.Topology;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.locationtech.jts.geom.Geometry;

public class ShapefileReaderTest {

  private ShapefileReader reader = new ShapefileReader(new File("src/test/resources/shapefile.zip"), new InMemory());

  @AfterEach
  public void close() {
    reader.close();
  }

  @Test
  public void testCount() {
    assertEquals(86, reader.getCount());
  }

  @Test
  @Timeout(30)
  public void testReadShapefile() {
    Map<String, Integer> counts = new TreeMap<>();
    List<Geometry> points = new ArrayList<>();
    Topology.start("test", new InMemory())
      .fromGenerator("shapefile", reader.read())
      .addBuffer("reader_queue", 100, 1)
      .sinkToConsumer("counter", 1, elem -> {
        String type = elem.getGeometry().getGeometryType();
        counts.put(type, counts.getOrDefault(type, 0) + 1);
        points.add(elem.getGeometry());
      }).await();
    assertEquals(86, points.size());
    var gc = GeoUtils.gf.createGeometryCollection(points.toArray(new Geometry[0]));
    var centroid = gc.getCentroid();
    assertEquals(-77.0297995, centroid.getX(), 5);
    assertEquals(38.9119684, centroid.getY(), 5);
  }
}
