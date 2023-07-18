package com.onthegomap.planetiler.reader;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.onthegomap.planetiler.TestUtils;
import com.onthegomap.planetiler.geo.GeoUtils;
import com.onthegomap.planetiler.stats.Stats;
import com.onthegomap.planetiler.util.FileUtils;
import com.onthegomap.planetiler.worker.WorkerPipeline;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.geotools.data.DefaultTransaction;
import org.geotools.data.shapefile.ShapefileDataStore;
import org.geotools.data.shapefile.ShapefileDataStoreFactory;
import org.geotools.data.simple.SimpleFeatureStore;
import org.geotools.feature.DefaultFeatureCollection;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.geotools.referencing.CRS;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.junit.jupiter.api.io.TempDir;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Point;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.operation.TransformException;

class ShapefileReaderTest {
  @TempDir
  private Path tempDir;

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
      featureBuilder.add(TestUtils.newPoint(1, 2));
      featureBuilder.add(3);
      var feature = featureBuilder.buildFeature(null);
      collection.add(feature);
      featureStore.addFeatures(collection);
      transaction.commit();
    }

    try (var reader = new ShapefileReader(null, "test", shpPath)) {
      assertEquals(1, reader.getFeatureCount());
      List<SimpleFeature> features = new ArrayList<>();
      reader.readFeatures(features::add);
      assertEquals(10.5113, features.get(0).latLonGeometry().getCentroid().getX(), 1e-4);
      assertEquals(0, features.get(0).latLonGeometry().getCentroid().getY(), 1e-4);
      assertEquals(3, features.get(0).getTag("value"));
    }
  }

  private static void testReadShapefile(Path path) {
    try (var reader = new ShapefileReader(null, "test", path)) {

      for (int i = 1; i <= 2; i++) {
        assertEquals(86, reader.getFeatureCount());
        List<Geometry> points = new ArrayList<>();
        List<String> names = new ArrayList<>();
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
