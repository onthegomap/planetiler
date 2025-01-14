package com.onthegomap.planetiler.reader;

import static org.junit.jupiter.api.Assertions.assertEquals;
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

public class GeoJsonReaderTest {
    @Test
    void testReadGeoJson() throws IOException {
        Path path = TestUtils.pathToResource("geojson.geojson");
        try (var reader = new GeoJsonReader("test", path)) {
            assertEquals(3, reader.getFeatureCount());
            List<Geometry> points = new CopyOnWriteArrayList<>();
            List<String> names = new CopyOnWriteArrayList<>();
            WorkerPipeline.start("test", Stats.inMemory())
                .fromGenerator("source", reader::readFeatures)
                .addBuffer("reader_queue", 100, 1)
                .sinkToConsumer("counter", 1, elem -> {
                assertTrue(elem.getTag("name") instanceof String);
                assertEquals("test", elem.getSource());
                assertEquals("geojson", elem.getSourceLayer());
                points.add(elem.latLonGeometry());
                names.add(elem.getTag("name").toString());
                }).await();
            assertEquals(3, points.size());
            assertTrue(names.contains("line"));
            assertTrue(names.contains("point"));
            assertTrue(names.contains("polygon"));
            var gc = GeoUtils.JTS_FACTORY.createGeometryCollection(points.toArray(new Geometry[0]));
            var centroid = gc.getCentroid();
            assertEquals(100.5, centroid.getX(), 1e-5);
            assertEquals(0.5, centroid.getY(), 1e-5);
        }
    }
}

