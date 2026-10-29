package com.onthegomap.planetiler.reader;

import static com.onthegomap.planetiler.TestUtils.newPoint;
import static com.onthegomap.planetiler.TestUtils.rectangle;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.onthegomap.planetiler.TestUtils;
import com.onthegomap.planetiler.config.Bounds;
import com.onthegomap.planetiler.geo.GeoUtils;
import com.onthegomap.planetiler.stats.Stats;
import com.onthegomap.planetiler.util.FileUtils;
import com.onthegomap.planetiler.worker.WorkerPipeline;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import org.geotools.api.data.SimpleFeatureStore;
import org.geotools.api.referencing.FactoryException;
import org.geotools.api.referencing.operation.TransformException;
import org.geotools.data.DefaultTransaction;
import org.geotools.data.collection.ListFeatureCollection;
import org.geotools.data.shapefile.ShapefileDataStore;
import org.geotools.data.shapefile.ShapefileDataStoreFactory;
import org.geotools.feature.DefaultFeatureCollection;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.geotools.referencing.CRS;
import org.geotools.referencing.crs.DefaultGeographicCRS;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;

class ShapefileReaderTest {
  @TempDir
  private Path tempDir;
  private static final Envelope env = newPoint(-77.12911152370515, 38.79930767201779).getEnvelopeInternal();
  private static final int numInEnv = 18;
  static {
    env.expandBy(0.1);
  }

  @Test
  @Timeout(30)
  @DisabledOnOs(OS.WINDOWS) // the zip file doesn't fully close, which causes trouble running test on windows
  void testReadShapefileExtracted() throws IOException {
    var extracted = TestUtils.extractPathToResource(tempDir, "shapefile.zip");
    try (var fs = FileSystems.newFileSystem(extracted)) {
      var path = fs.getPath("shapefile", "stations.shp");
      testReadShapefile(path);
    }
  }

  @Test
  @Timeout(30)
  void testReadShapefileUnzipped() throws IOException {
    var dest = tempDir.resolve("shapefile.zip");
    FileUtils.unzipResource("/shapefile.zip", dest);
    testReadShapefile(dest.resolve("shapefile").resolve("stations.shp"));
  }

  @Test
  @Timeout(30)
  void testReadShapefileWithBoundingBox() {
    var dest = tempDir.resolve("shapefile.zip");
    FileUtils.unzipResource("/shapefile.zip", dest);
    try (
      var reader = new ShapefileReader(null, "test", dest.resolve("shapefile").resolve("stations.shp"), new Bounds(env))
    ) {
      for (int i = 1; i <= 2; i++) {
        assertEquals(numInEnv, reader.getFeatureCount());
        List<Geometry> points = new CopyOnWriteArrayList<>();
        WorkerPipeline.start("test", Stats.inMemory())
          .fromGenerator("source", reader::readFeatures)
          .addBuffer("reader_queue", 100, 1)
          .sinkToConsumer("counter", 1, elem -> {
            assertTrue(elem.getTag("name") instanceof String);
            assertEquals("test", elem.getSource());
            assertEquals("stations", elem.getSourceLayer());
            points.add(elem.latLonGeometry());
          }).await();
        assertEquals(numInEnv, points.size());
        var gc = GeoUtils.JTS_FACTORY.createGeometryCollection(points.toArray(new Geometry[0]));
        var centroid = gc.getCentroid();
        assertEquals(-77.0934256, centroid.getX(), 1e-5, "iter " + i);
        assertEquals(38.8509022, centroid.getY(), 1e-5, "iter " + i);
      }
    }
  }

  @Test
  void testReadShapefileLeniently(@TempDir Path dir) throws IOException, TransformException, FactoryException {
    var shpPath = dir.resolve("test.shp");
    var dataStoreFactory = new ShapefileDataStoreFactory();
    var newDataStore =
      (ShapefileDataStore) dataStoreFactory.createNewDataStore(Map.of("url", shpPath.toUri().toURL()));

    var builder = new SimpleFeatureTypeBuilder();
    builder.setName("the_geom");
    builder.setCRS(CRS.parseWKT(
      """
        PROJCS["SWEREF99_TM",GEOGCS["GCS_SWEREF99",DATUM["D_SWEREF99",SPHEROID["GRS_1980",6378137.0,298.257222101]],PRIMEM["Greenwich",0.0],UNIT["Degree",0.0174532925199433]],PROJECTION["Transverse_Mercator"],PARAMETER["False_Easting",500000.0],PARAMETER["False_Northing",0.0],PARAMETER["Central_Meridian",15.0],PARAMETER["Scale_Factor",0.9996],PARAMETER["Latitude_Of_Origin",0.0],UNIT["Meter",1.0]]
        """));

    builder.add("the_geom", Point.class);
    builder.add("value", Integer.class);
    builder.setDefaultGeometry("the_geom");
    var type = builder.buildFeatureType();
    newDataStore.createSchema(type);

    try (var transaction = new DefaultTransaction("create")) {
      var typeName = newDataStore.getTypeNames()[0];
      var featureSource = newDataStore.getFeatureSource(typeName);
      var featureStore = (SimpleFeatureStore) featureSource;
      featureStore.setTransaction(transaction);
      var collection = new DefaultFeatureCollection();
      var featureBuilder = new SimpleFeatureBuilder(type);
      featureBuilder.add(newPoint(1, 2));
      featureBuilder.add(3);
      var feature = featureBuilder.buildFeature(null);
      collection.add(feature);
      featureStore.addFeatures(collection);
      transaction.commit();
    }

    try (var reader = new ShapefileReader(null, "test", shpPath)) {
      assertEquals(1, reader.getFeatureCount());
      List<SimpleFeature> features = new CopyOnWriteArrayList<>();
      reader.readFeatures(features::add);
      assertEquals(10.5113, features.getFirst().latLonGeometry().getCentroid().getX(), 1e-4);
      assertEquals(0, features.getFirst().latLonGeometry().getCentroid().getY(), 1e-4);
      assertEquals(3, features.getFirst().getTag("value"));
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void testSkipsRecordWithGeometryThatCantBeRead(boolean withBounds, @TempDir Path dir)
    throws IOException, TransformException {
    // write 3 polygons with value 1, 2 and 3, then set the second one's point count to 0 like the polygons with 1 ring
    // and no points in Geofabrik extracts (#1568) that make GeoTools throw while decoding them
    var shpPath = writePolygons(dir.resolve("test.shp"), 3);
    var shp = ByteBuffer.wrap(Files.readAllBytes(shpPath));
    // records start after the 100-byte header with an 8-byte header holding their length in 16-bit words
    int secondRecord = 100 + 8 + shp.getInt(100 + 4) * 2;
    // the point count follows the shape type, bounding box and part count
    shp.order(ByteOrder.LITTLE_ENDIAN).putInt(secondRecord + 8 + 40, 0);
    Files.write(shpPath, shp.array());

    // counting within bounds used to decode every geometry, so it failed on the unreadable record too
    var bounds = withBounds ? new Bounds(new Envelope(0, 10, 0, 10)) : Bounds.WORLD;
    try (var reader = new ShapefileReader(null, "test", shpPath, bounds)) {
      assertEquals(3, reader.getFeatureCount());
      List<SimpleFeature> features = new CopyOnWriteArrayList<>();
      reader.readFeatures(features::add);
      assertEquals(2, features.size());
      assertEquals(1, features.get(0).getTag("value"));
      assertEquals(rectangle(1, 1.5).getEnvelopeInternal(), features.get(0).latLonGeometry().getEnvelopeInternal());
      assertEquals(3, features.get(1).getTag("value"));
      assertEquals(rectangle(3, 3.5).getEnvelopeInternal(), features.get(1).latLonGeometry().getEnvelopeInternal());
    }
  }

  @Test
  void testFailsWhenDbfHasFewerRecordsThanShp(@TempDir Path dir) throws IOException {
    var shpPath = writePolygons(dir.resolve("test.shp"), 3);
    writePolygons(dir.resolve("other.shp"), 2);
    Files.copy(dir.resolve("other.dbf"), dir.resolve("test.dbf"), StandardCopyOption.REPLACE_EXISTING);
    try (var reader = new ShapefileReader(null, "test", shpPath)) {
      assertThrows(RuntimeException.class, () -> reader.readFeatures(feature -> {
      }));
    }
  }

  @Test
  void testIgnoresExtraDbfRecords(@TempDir Path dir) throws IOException, TransformException {
    var shpPath = writePolygons(dir.resolve("test.shp"), 3);
    writePolygons(dir.resolve("other.shp"), 4);
    Files.copy(dir.resolve("other.dbf"), dir.resolve("test.dbf"), StandardCopyOption.REPLACE_EXISTING);
    try (var reader = new ShapefileReader(null, "test", shpPath)) {
      List<SimpleFeature> features = new CopyOnWriteArrayList<>();
      reader.readFeatures(features::add);
      assertEquals(3, features.size());
      for (int i = 1; i <= 3; i++) {
        var feature = features.get(i - 1);
        assertEquals(i, feature.getTag("value"));
        assertEquals(rectangle(i, i + 0.5).getEnvelopeInternal(), feature.latLonGeometry().getEnvelopeInternal());
      }
    }
  }

  /** Writes polygons with {@code value} 1 to {@code count} in order, the i-th covering (i, i) to (i + 0.5, i + 0.5). */
  private static Path writePolygons(Path shpPath, int count) throws IOException {
    var dataStore = (ShapefileDataStore) new ShapefileDataStoreFactory()
      .createNewDataStore(Map.of("url", shpPath.toUri().toURL()));
    var builder = new SimpleFeatureTypeBuilder();
    builder.setName("the_geom");
    builder.setCRS(DefaultGeographicCRS.WGS84);
    builder.add("the_geom", Polygon.class);
    builder.add("value", Integer.class);
    builder.setDefaultGeometry("the_geom");
    var type = builder.buildFeatureType();
    dataStore.createSchema(type);
    try (var transaction = new DefaultTransaction("create")) {
      var featureStore = (SimpleFeatureStore) dataStore.getFeatureSource(dataStore.getTypeNames()[0]);
      featureStore.setTransaction(transaction);
      // ListFeatureCollection keeps the order, DefaultFeatureCollection sorts by generated feature ID
      var collection = new ListFeatureCollection(type);
      for (int i = 1; i <= count; i++) {
        collection.add(SimpleFeatureBuilder.build(type, new Object[]{rectangle(i, i + 0.5), i}, null));
      }
      featureStore.addFeatures(collection);
      transaction.commit();
    }
    dataStore.dispose();
    return shpPath;
  }

  private static void testReadShapefile(Path path) {
    try (var reader = new ShapefileReader(null, "test", path)) {

      for (int i = 1; i <= 2; i++) {
        assertEquals(86, reader.getFeatureCount());
        List<Geometry> points = new CopyOnWriteArrayList<>();
        List<String> names = new CopyOnWriteArrayList<>();
        WorkerPipeline.start("test", Stats.inMemory())
          .fromGenerator("source", reader::readFeatures)
          .addBuffer("reader_queue", 100, 1)
          .sinkToConsumer("counter", 1, elem -> {
            assertTrue(elem.getTag("name") instanceof String);
            assertEquals("test", elem.getSource());
            assertEquals("stations", elem.getSourceLayer());
            points.add(elem.latLonGeometry());
            names.add(elem.getTag("name").toString());
          }).await();
        assertEquals(numInEnv, points.stream().filter(point -> env.contains(point.getCoordinate())).count());
        assertEquals(86, points.size());
        assertTrue(names.contains("Van Dörn Street"));
        var gc = GeoUtils.JTS_FACTORY.createGeometryCollection(points.toArray(new Geometry[0]));
        var centroid = gc.getCentroid();
        assertEquals(-77.0297995, centroid.getX(), 1e-5, "iter " + i);
        assertEquals(38.9119684, centroid.getY(), 1e-5, "iter " + i);
      }
    }
  }
}
