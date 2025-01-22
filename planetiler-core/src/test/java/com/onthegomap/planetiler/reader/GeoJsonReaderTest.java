package com.onthegomap.planetiler.reader;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.onthegomap.planetiler.TestUtils;
import com.onthegomap.planetiler.geo.GeoUtils;
import com.onthegomap.planetiler.stats.Stats;
import com.onthegomap.planetiler.worker.WorkerPipeline;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import org.junit.jupiter.api.Test;
import org.locationtech.jts.geom.Geometry;

class GeoJsonReaderTest {
  List<Geometry> features = new CopyOnWriteArrayList<>();
  List<String> names = new CopyOnWriteArrayList<>();

  @Test
  void testReadFeatureCollection() throws IOException {
    readFile("featurecollection.geojson", "featurecollection", 3);
    assertTrue(names.contains("line"));
    assertTrue(names.contains("point"));
    assertTrue(names.contains("polygon"));
    var gc = GeoUtils.JTS_FACTORY.createGeometryCollection(features.toArray(Geometry[]::new));
    var centroid = gc.getCentroid();
    assertEquals(100.5, centroid.getX(), 1e-5);
    assertEquals(0.5, centroid.getY(), 1e-5);
  }

  @Test
  void testReadNewlineDelimited() throws IOException {
    readFile("newlines.geojson", "newlines", 3);
    assertTrue(names.contains("line"));
    assertTrue(names.contains("point"));
    assertTrue(names.contains("polygon"));
    var gc = GeoUtils.JTS_FACTORY.createGeometryCollection(features.toArray(Geometry[]::new));
    var centroid = gc.getCentroid();
    assertEquals(100.5, centroid.getX(), 1e-5);
    assertEquals(0.5, centroid.getY(), 1e-5);
  }

  @Test
  void testReadFeature() throws IOException {
    readFile("feature.geojson", "feature", 1);
    var gc = GeoUtils.JTS_FACTORY.createGeometryCollection(features.toArray(Geometry[]::new));
    var centroid = gc.getCentroid();
    assertEquals(102, centroid.getX(), 1e-5);
    assertEquals(0.5, centroid.getY(), 1e-5);
  }

  private void readFile(String name, String layer, int expectedFeatures) throws IOException {
    Path path = TestUtils.pathToResource(name);
    try (var reader = new GeoJsonReader("test", path)) {
      assertEquals(expectedFeatures, reader.getFeatureCount());
      WorkerPipeline.start("test", Stats.inMemory())
        .fromGenerator("source", reader::readFeatures)
        .addBuffer("reader_queue", 100, 1)
        .sinkToConsumer("counter", 1, elem -> {
          assertInstanceOf(String.class, elem.getTag("name"));
          assertEquals("test", elem.getSource());
          assertEquals(layer, elem.getSourceLayer());
          features.add(elem.latLonGeometry());
          names.add(elem.getTag("name").toString());
        }).await();
    }
    assertEquals(expectedFeatures, features.size());
  }
}

