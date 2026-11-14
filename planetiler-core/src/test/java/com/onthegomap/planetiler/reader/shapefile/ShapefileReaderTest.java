package com.onthegomap.planetiler.reader.shapefile;

import static com.onthegomap.planetiler.TestUtils.newPoint;
import static com.onthegomap.planetiler.TestUtils.rectangle;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.onthegomap.planetiler.TestUtils;
import com.onthegomap.planetiler.config.Bounds;
import com.onthegomap.planetiler.geo.GeoUtils;
import com.onthegomap.planetiler.geo.GeometryException;
import com.onthegomap.planetiler.reader.FileFormatException;
import com.onthegomap.planetiler.reader.ShapefileOptions;
import com.onthegomap.planetiler.reader.SimpleFeature;
import com.onthegomap.planetiler.stats.Stats;
import com.onthegomap.planetiler.util.FileUtils;
import com.onthegomap.planetiler.worker.WorkerPipeline;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;

class ShapefileReaderTest {
  @TempDir
  private Path tempDir;
  private static final Envelope env = newPoint(-77.12911152370515, 38.79930767201779).getEnvelopeInternal();
  private static final int NUM_IN_ENV = 18;
  static {
    env.expandBy(0.1);
  }

  @Test
  @Timeout(30)
  void testReadShapefileExtracted() throws IOException {
    var extracted = TestUtils.extractPathToResource(tempDir, "shapefile.zip");
    try (var fs = FileSystems.newFileSystem(extracted)) {
      var path = fs.getPath("shapefile", "stations.shp");
      testReadShapefile(path);
    }
  }

  @Test
  @Timeout(30)
  void testReadShapefileUnzipped() {
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
        assertEquals(NUM_IN_ENV, reader.getFeatureCount());
        List<Geometry> points = new CopyOnWriteArrayList<>();
        WorkerPipeline.start("test", Stats.inMemory())
          .fromGenerator("source", reader::readFeatures)
          .addBuffer("reader_queue", 100, 1)
          .sinkTo("counter", 1, prev -> {
            for (var elem : prev) {
              assertTrue(elem.getTag("name") instanceof String);
              assertEquals("test", elem.getSource());
              assertEquals("stations", elem.getSourceLayer());
              points.add(elem.latLonGeometry());
            }
          }).await();
        assertEquals(NUM_IN_ENV, points.size());
        var gc = GeoUtils.JTS_FACTORY.createGeometryCollection(points.toArray(new Geometry[0]));
        var centroid = gc.getCentroid();
        assertEquals(-77.0934256, centroid.getX(), 1e-5, "iter " + i);
        assertEquals(38.8509022, centroid.getY(), 1e-5, "iter " + i);
      }
    }
  }

  @Test
  void testReadShapefileLeniently(@TempDir Path dir) throws IOException, GeometryException {
    var shpPath = TestShapefileWriter.write(dir.resolve("test.shp"),
      """
        PROJCS["SWEREF99_TM",GEOGCS["GCS_SWEREF99",DATUM["D_SWEREF99",SPHEROID["GRS_1980",6378137.0,298.257222101]],PRIMEM["Greenwich",0.0],UNIT["Degree",0.0174532925199433]],PROJECTION["Transverse_Mercator"],PARAMETER["False_Easting",500000.0],PARAMETER["False_Northing",0.0],PARAMETER["Central_Meridian",15.0],PARAMETER["Scale_Factor",0.9996],PARAMETER["Latitude_Of_Origin",0.0],UNIT["Meter",1.0]]
        """,
      List.of(newPoint(1, 2)), List.of("value"), List.of(List.of(3)));

    try (var reader = new ShapefileReader(null, "test", shpPath)) {
      assertEquals(1, reader.getFeatureCount());
      List<ShapefileFeature> features = new CopyOnWriteArrayList<>();
      reader.readFeatures(features::add);
      assertEquals(10.5113, features.getFirst().latLonGeometry().getCentroid().getX(), 1e-4);
      assertEquals(0, features.getFirst().latLonGeometry().getCentroid().getY(), 1e-4);
      assertEquals(3, features.getFirst().getTag("value"));
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void testSkipsRecordWithGeometryThatCantBeRead(boolean withBounds, @TempDir Path dir)
    throws IOException, GeometryException {
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
      List<ShapefileFeature> features = new CopyOnWriteArrayList<>();
      reader.readFeatures(features::add);
      // geometries are only decoded when requested, so the unreadable one only fails then
      assertEquals(3, features.size());
      assertEquals(1, features.get(0).getTag("value"));
      assertEquals(rectangle(1, 1.5).getEnvelopeInternal(), features.get(0).latLonGeometry().getEnvelopeInternal());
      assertEquals(2, features.get(1).getTag("value"));
      var unreadable = features.get(1);
      assertThrows(GeometryException.class, unreadable::latLonGeometry);
      assertEquals(3, features.get(2).getTag("value"));
      assertEquals(rectangle(3, 3.5).getEnvelopeInternal(), features.get(2).latLonGeometry().getEnvelopeInternal());
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
  void testIgnoresExtraDbfRecords(@TempDir Path dir) throws IOException, GeometryException {
    var shpPath = writePolygons(dir.resolve("test.shp"), 3);
    writePolygons(dir.resolve("other.shp"), 4);
    Files.copy(dir.resolve("other.dbf"), dir.resolve("test.dbf"), StandardCopyOption.REPLACE_EXISTING);
    try (var reader = new ShapefileReader(null, "test", shpPath)) {
      List<ShapefileFeature> features = new CopyOnWriteArrayList<>();
      reader.readFeatures(features::add);
      assertEquals(3, features.size());
      for (int i = 1; i <= 3; i++) {
        var feature = features.get(i - 1);
        assertEquals(i, feature.getTag("value"));
        assertEquals(rectangle(i, i + 0.5).getEnvelopeInternal(), feature.latLonGeometry().getEnvelopeInternal());
      }
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void testAttributeOptions(boolean withOptions, @TempDir Path dir) throws IOException {
    var shpPath = TestShapefileWriter.write(dir.resolve("test.shp"), TestShapefileWriter.WGS84_PRJ,
      List.of(newPoint(1, 2), newPoint(3, 4)), List.of("NAME", "Rank"), List.of(List.of("a", 1), List.of("", 2)));

    var options =
      withOptions ? ShapefileOptions.DEFAULT.withLowercaseAttributeNames(true).withEmptyStringsAsNull(true) :
        ShapefileOptions.DEFAULT;
    try (var reader = new ShapefileReader("test", shpPath, Bounds.WORLD, options)) {
      List<ShapefileFeature> features = new CopyOnWriteArrayList<>();
      reader.readFeatures(features::add);
      assertEquals(2, features.size());
      if (withOptions) {
        assertEquals(Map.of("name", "a", "rank", 1), features.get(0).tags());
        assertEquals(Map.of("rank", 2), features.get(1).tags());
      } else {
        assertEquals(Map.of("NAME", "a", "Rank", 1), features.get(0).tags());
        assertEquals(Map.of("NAME", "", "Rank", 2), features.get(1).tags());
      }
    }
  }

  /** Writes polygons with {@code value} 1 to {@code count} in order, the i-th covering (i, i) to (i + 0.5, i + 0.5). */
  private static Path writePolygons(Path shpPath, int count) throws IOException {
    List<Geometry> polygons = new ArrayList<>();
    for (int i = 1; i <= count; i++) {
      polygons.add(rectangle(i, i + 0.5));
    }
    return TestShapefileWriter.write(shpPath, polygons);
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
          .sinkTo("counter", 1, prev -> {
            for (var elem : prev) {
              assertTrue(elem.getTag("name") instanceof String);
              assertEquals("test", elem.getSource());
              assertEquals("stations", elem.getSourceLayer());
              points.add(elem.latLonGeometry());
              names.add(elem.getTag("name").toString());
            }
          }).await();
        assertEquals(NUM_IN_ENV, points.stream().filter(point -> env.contains(point.getCoordinate())).count());
        assertEquals(86, points.size());
        assertTrue(names.contains("Van Dörn Street"));
        var gc = GeoUtils.JTS_FACTORY.createGeometryCollection(points.toArray(new Geometry[0]));
        var centroid = gc.getCentroid();
        assertEquals(-77.0297995, centroid.getX(), 1e-5, "iter " + i);
        assertEquals(38.9119684, centroid.getY(), 1e-5, "iter " + i);
      }
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {
    "POINT (1 2)", "MULTIPOINT ((1 2), (3 4))", "LINESTRING (1 2, 3 4, 5 6)", "POLYGON ((1 1, 1 2, 2 2, 2 1, 1 1))",
    "POINT Z (1 2 3)", "MULTIPOINT Z ((1 2 3), (3 4 5))", "LINESTRING Z (1 2 3, 3 4 5, 5 6 7)",
    "POLYGON Z ((1 1 3, 1 2 3, 2 2 3, 2 1 3, 1 1 3))"
  })
  void testGeometryTypeAndSizeWithoutDecoding(String wkt, @TempDir Path dir)
    throws IOException, GeometryException, ParseException {
    Geometry geometry = new WKTReader().read(wkt);
    var shpPath = TestShapefileWriter.write(dir.resolve("test.shp"), List.of(geometry));
    try (var reader = new ShapefileReader(null, "test", shpPath)) {
      List<ShapefileFeature> features = new CopyOnWriteArrayList<>();
      reader.readFeatures(features::add);
      var feature = features.getFirst();
      var decoded = SimpleFeature.fromLatLonGeometry(feature.latLonGeometry());
      assertEquals(decoded.isPoint(), feature.isPoint());
      assertEquals(decoded.canBeLine(), feature.canBeLine());
      assertEquals(decoded.canBePolygon(), feature.canBePolygon());
      assertEquals(decoded.numPoints(), feature.numPoints());
      assertTrue(geometry.equalsTopo(feature.latLonGeometry()));
      assertEquals(geometry.getCoordinate().getZ(), feature.latLonGeometry().getCoordinate().getZ());
    }
  }

  @Test
  void testStopsAtTruncatedRecord(@TempDir Path dir) throws IOException, GeometryException {
    var shpPath = writePolygons(dir.resolve("test.shp"), 3);
    byte[] shp = Files.readAllBytes(shpPath);
    Files.write(shpPath, Arrays.copyOf(shp, shp.length - 10));
    try (var reader = new ShapefileReader(null, "test", shpPath)) {
      List<ShapefileFeature> features = new CopyOnWriteArrayList<>();
      reader.readFeatures(features::add);
      assertEquals(2, features.size());
      assertEquals(rectangle(2, 2.5).getEnvelopeInternal(), features.get(1).latLonGeometry().getEnvelopeInternal());
    }
  }

  @Test
  void testSkipsDeletedRecords(@TempDir Path dir) throws IOException {
    var shpPath = writePolygons(dir.resolve("test.shp"), 3);
    // dBase marks a deleted record with '*' in its first byte, right after the header
    var dbf = dir.resolve("test.dbf");
    byte[] bytes = Files.readAllBytes(dbf);
    int headerLength = ByteBuffer.wrap(bytes, 8, 2).order(ByteOrder.LITTLE_ENDIAN).getShort();
    bytes[headerLength] = '*';
    Files.write(dbf, bytes);
    try (var reader = new ShapefileReader(null, "test", shpPath)) {
      List<ShapefileFeature> features = new CopyOnWriteArrayList<>();
      reader.readFeatures(features::add);
      assertEquals(List.of(2, 3), features.stream().map(f -> f.getTag("value")).toList());
    }
  }

  private static List<ShapefileFeature> readAll(Path shpPath, String projection) throws IOException {
    try (var reader = new ShapefileReader(projection, "test", shpPath)) {
      List<ShapefileFeature> features = new CopyOnWriteArrayList<>();
      reader.readFeatures(features::add);
      return features;
    }
  }

  private static void assertLonLat(double lon, double lat, ShapefileFeature feature) throws GeometryException {
    var point = feature.latLonGeometry().getCoordinate();
    assertEquals(lon, point.x, 1e-7, point::toString);
    assertEquals(lat, point.y, 1e-7, point::toString);
  }

  @Test
  void testReprojectsProjectedPrj(@TempDir Path dir) throws IOException, GeometryException {
    String utm33 =
      """
        PROJCS["WGS_1984_UTM_Zone_33N",GEOGCS["GCS_WGS_1984",DATUM["D_WGS_1984",SPHEROID["WGS_1984",6378137.0,298.257223563]],PRIMEM["Greenwich",0.0],UNIT["Degree",0.0174532925199433]],PROJECTION["Transverse_Mercator"],PARAMETER["False_Easting",500000.0],PARAMETER["False_Northing",0.0],PARAMETER["Central_Meridian",15.0],PARAMETER["Scale_Factor",0.9996],PARAMETER["Latitude_Of_Origin",0.0],UNIT["Meter",1.0]]
        """;
    // PROJ 9.6
    var shpPath = TestShapefileWriter.write(dir.resolve("test.shp"), utm33,
      List.of(newPoint(500000, 5538630.7029), newPoint(535833.4592, 5538750.4776)), List.of("value"),
      List.of(List.of(1), List.of(2)));
    var features = readAll(shpPath, null);
    assertLonLat(15, 50, features.get(0));
    assertLonLat(15.5, 50, features.get(1));
  }

  // EPSG:4269 (NAD83) is latitude first, but shapefile coordinates follow the .prj they come with, east / north here
  @ParameterizedTest
  @ValueSource(strings = {"EPSG:4269", "EPSG:4269:lon_first"})
  void testOverrideKeepsAxisOrderOfWgs84Prj(String projection, @TempDir Path dir)
    throws IOException, GeometryException {
    var shpPath = TestShapefileWriter.write(dir.resolve("test.shp"), List.of(newPoint(-71.1, 42.3)));
    assertLonLat(-71.1, 42.3, readAll(shpPath, projection).getFirst());
  }

  @Test
  void testWithoutPrjIsLonLat(@TempDir Path dir) throws IOException, GeometryException {
    var shpPath = TestShapefileWriter.write(dir.resolve("test.shp"), null, List.of(newPoint(-71.1, 42.3)),
      List.of("value"), List.of(List.of(1)));
    assertLonLat(-71.1, 42.3, readAll(shpPath, null).getFirst());
  }

  @ParameterizedTest
  @CsvSource({
    "EPSG:4326, 42.3, -71.1",
    "EPSG:4326:lon_first, -71.1, 42.3",
    "CRS:84, -71.1, 42.3",
  })
  void testWgs84OverrideWithoutPrjUsesItsAxisOrder(String projection, double x, double y, @TempDir Path dir)
    throws IOException, GeometryException {
    var shpPath = TestShapefileWriter.write(dir.resolve("test.shp"), null, List.of(newPoint(x, y)),
      List.of("value"), List.of(List.of(1)));
    assertLonLat(-71.1, 42.3, readAll(shpPath, projection).getFirst());
  }

  @ParameterizedTest
  @CsvSource(value = {
    "UTF-8, é",
    "65001, é",
    "utf8, é",
    "1252, Ã©",
    "unknown-charset, Ã©",
    "NONE, Ã©",
  })
  void testCpgCharset(String cpg, String expected, @TempDir Path dir) throws IOException {
    // the test writer encodes text as UTF-8
    var shpPath = TestShapefileWriter.write(dir.resolve("test.shp"), TestShapefileWriter.WGS84_PRJ,
      List.of(newPoint(1, 2)), List.of("name"), List.of(List.of("é")));
    var cpgPath = dir.resolve("test.cpg");
    if (cpg.equals("NONE")) {
      Files.delete(cpgPath);
    } else {
      Files.writeString(cpgPath, cpg);
    }
    assertEquals(expected, readAll(shpPath, null).getFirst().getTag("name"));
  }

  @Test
  void testUpperCaseFileNames(@TempDir Path dir) throws IOException, GeometryException {
    TestShapefileWriter.write(dir.resolve("test.shp"), List.of(newPoint(1, 2)));
    for (String extension : List.of("shp", "shx", "dbf", "prj", "cpg")) {
      Files.move(dir.resolve("test." + extension), dir.resolve("TEST." + extension.toUpperCase()));
    }
    var feature = readAll(dir.resolve("TEST.SHP"), null).getFirst();
    assertEquals("TEST", feature.getSourceLayer());
    assertEquals(1, feature.getTag("value"));
    assertLonLat(1, 2, feature);
  }

  @Test
  void testReopensClosedZipFileSystem(@TempDir Path dir) throws IOException, GeometryException {
    TestShapefileWriter.write(dir.resolve("test.shp"), List.of(newPoint(1, 2)));
    Path zip = dir.resolve("test.zip");
    try (var zipFs = FileSystems.newFileSystem(zip, Map.of("create", "true"))) {
      for (String extension : List.of("shp", "shx", "dbf", "prj", "cpg")) {
        Files.copy(dir.resolve("test." + extension), zipFs.getPath("test." + extension));
      }
    }
    Path inZip;
    try (var zipFs = FileSystems.newFileSystem(zip)) {
      inZip = zipFs.getPath("test.shp");
    }
    assertLonLat(1, 2, readAll(inZip, null).getFirst());
  }

  @Test
  void testBoundsUseProjectionOverride(@TempDir Path dir) throws IOException, GeometryException {
    // UTM zone 33N points at 15°E / 15.5°E, 50°N (PROJ 9.6), in a shapefile without a .prj
    var shpPath = TestShapefileWriter.write(dir.resolve("test.shp"), null,
      List.of(newPoint(500000, 5538630.7029), newPoint(535833.4592, 5538750.4776)), List.of("value"),
      List.of(List.of(1), List.of(2)));
    var bounds = new Bounds(new Envelope(14.9, 15.1, 49.9, 50.1));
    var options = ShapefileOptions.builder().projection("EPSG:32633").build();
    try (var reader = new ShapefileReader("test", shpPath, bounds, options)) {
      assertEquals(1, reader.getFeatureCount());
      List<ShapefileFeature> features = new CopyOnWriteArrayList<>();
      reader.readFeatures(features::add);
      assertEquals(1, features.size());
      assertLonLat(15, 50, features.getFirst());
    }
  }

  @Test
  void testUnreadablePrjFailsWithoutProjectionOverride(@TempDir Path dir) throws IOException {
    var shpPath = TestShapefileWriter.write(dir.resolve("test.shp"), "not a projection", List.of(newPoint(1, 2)),
      List.of("value"), List.of(List.of(1)));
    assertThrows(FileFormatException.class, () -> new ShapefileReader(null, "test", shpPath));
  }

  @Test
  void testProjectionOverrideReplacesUnreadablePrj(@TempDir Path dir) throws IOException, GeometryException {
    var shpPath = TestShapefileWriter.write(dir.resolve("test.shp"), "not a projection", List.of(newPoint(-71.1, 42.3)),
      List.of("value"), List.of(List.of(1)));
    assertLonLat(-71.1, 42.3, readAll(shpPath, "CRS:84").getFirst());
  }

  @Test
  void testParseHeaders(@TempDir Path dir) throws IOException {
    var shpPath = TestShapefileWriter.write(dir.resolve("test.shp"), List.of(newPoint(1, 2), newPoint(3, 4)));
    byte[] bytes = Files.readAllBytes(shpPath);
    var header = ShpHeader.parse(bytes);
    assertEquals(new ShpHeader(bytes.length, ShapeType.POINT.id, new Envelope(1, 3, 2, 4)), header);
    var firstRecord = ShpRecordHeader.parse(Arrays.copyOfRange(bytes, ShpHeader.BYTES, bytes.length));
    assertEquals(new ShpRecordHeader(1, 20), firstRecord);
    assertEquals(2, ShpHeader.recordsInIndexFileFromSize(Files.size(dir.resolve("test.shx"))));
  }

  @ParameterizedTest
  @ValueSource(strings = {"NONE", "unknown-charset", "1252"})
  void testCharsetOptionOverridesCpg(String cpg, @TempDir Path dir) throws IOException {
    // the test writer encodes text as UTF-8
    var shpPath = TestShapefileWriter.write(dir.resolve("test.shp"), TestShapefileWriter.WGS84_PRJ,
      List.of(newPoint(1, 2)), List.of("name"), List.of(List.of("é")));
    var cpgPath = dir.resolve("test.cpg");
    if (cpg.equals("NONE")) {
      Files.delete(cpgPath);
    } else {
      Files.writeString(cpgPath, cpg);
    }
    var options = ShapefileOptions.builder().charset(StandardCharsets.UTF_8).build();
    try (var reader = new ShapefileReader("test", shpPath, Bounds.WORLD, options)) {
      List<ShapefileFeature> features = new CopyOnWriteArrayList<>();
      reader.readFeatures(features::add);
      assertEquals("é", features.getFirst().getTag("name"));
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {
    "POINT Z (1 2 3)",
    "POINT M (1 2 3)",
    "POINT ZM (1 2 3 4)",
    "MULTIPOINT M ((1 2 3), (4 5 6))",
    "MULTIPOINT ZM ((1 2 3 4), (4 5 6 7))",
    "LINESTRING M (1 2 3, 4 5 6)",
    "LINESTRING ZM (1 2 3 4, 4 5 6 7)",
    "POLYGON M ((0 0 1, 0 1 2, 1 1 3, 1 0 4, 0 0 1))",
    "POLYGON ZM ((0 0 1 5, 0 1 2 6, 1 1 3 7, 1 0 4 8, 0 0 1 5))",
  })
  void testReadsZAndMValues(String wkt, @TempDir Path dir) throws IOException, ParseException, GeometryException {
    Geometry geometry = new WKTReader().read(wkt);
    var shpPath = TestShapefileWriter.write(dir.resolve("test.shp"), List.of(geometry));
    Geometry read = readAll(shpPath, null).getFirst().latLonGeometry();
    assertEquals(coordinatesWithZm(geometry), coordinatesWithZm(read));
  }

  @Test
  void testMNoDataValueReadsAsNaN(@TempDir Path dir) throws IOException, ParseException, GeometryException {
    Geometry geometry = new WKTReader().read("LINESTRING M (1 2 -1e39, 3 4 5)");
    var shpPath = TestShapefileWriter.write(dir.resolve("test.shp"), List.of(geometry));
    var coordinates = readAll(shpPath, null).getFirst().latLonGeometry().getCoordinates();
    assertTrue(Double.isNaN(coordinates[0].getM()));
    assertEquals(5, coordinates[1].getM());
  }

  @Test
  void testReprojectsGeometryWithM(@TempDir Path dir) throws IOException, ParseException, GeometryException {
    Geometry geometry = new WKTReader().read("POINT M (500000 5538630.7029 3)");
    var shpPath = TestShapefileWriter.write(dir.resolve("test.shp"), null, List.of(geometry), List.of("value"),
      List.of(List.of(1)));
    assertLonLat(15, 50, readAll(shpPath, "EPSG:32633").getFirst());
  }

  private static List<String> coordinatesWithZm(Geometry geometry) {
    return Arrays.stream(geometry.getCoordinates())
      .map(c -> c.x + " " + c.y + " " + c.getZ() + " " + c.getM())
      .sorted()
      .toList();
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void testReadsWithoutAttributes(boolean withDbf, @TempDir Path dir) throws IOException {
    var shpPath = TestShapefileWriter.write(dir.resolve("test.shp"), TestShapefileWriter.WGS84_PRJ,
      List.of(newPoint(1, 2), newPoint(3, 4)), List.of(), List.of(List.of(), List.of()));
    if (!withDbf) {
      Files.delete(dir.resolve("test.dbf"));
    }
    assertEquals(List.of(Map.of(), Map.of()), readAll(shpPath, null).stream().map(ShapefileFeature::tags).toList());
  }

  @Test
  void testSkipsNullShapeRecords(@TempDir Path dir) throws IOException {
    var shpPath = writePoints(dir.resolve("test.shp"), 3);
    setSecondPointShapeType(shpPath, ShapeType.NULL.id);
    // the null record's .dbf row is skipped with it
    assertEquals(List.of(1, 3), values(readAll(shpPath, null)));
  }

  @Test
  void testRecordWithUnknownShapeTypeFailsToDecode(@TempDir Path dir) throws IOException {
    var shpPath = writePoints(dir.resolve("test.shp"), 3);
    setSecondPointShapeType(shpPath, 99);
    var features = readAll(shpPath, null);
    assertEquals(List.of(1, 2, 3), values(features));
    var unknown = features.get(1);
    assertFalse(unknown.isPoint() || unknown.canBeLine() || unknown.canBePolygon());
    assertThrows(GeometryException.class, unknown::latLonGeometry);
  }

  @Test
  void testSkipsRecordWithoutShapeType(@TempDir Path dir) throws IOException {
    var shpPath = writePolygons(dir.resolve("test.shp"), 3);
    shortenLastRecord(shpPath, 2);
    assertEquals(List.of(1, 2), values(readAll(shpPath, null)));
  }

  @ParameterizedTest
  @CsvSource({"true, 8", "false, 32"})
  void testRecordTooShortForItsBoundsFailsToDecode(boolean points, int contentBytes, @TempDir Path dir)
    throws IOException {
    var shpPath = points ? writePoints(dir.resolve("test.shp"), 3) : writePolygons(dir.resolve("test.shp"), 3);
    shortenLastRecord(shpPath, contentBytes);
    // the bounds filter can't tell if the record is in bounds, so it passes it on for decoding to report
    try (var reader = new ShapefileReader(null, "test", shpPath, new Bounds(new Envelope(0, 10, 0, 10)))) {
      List<ShapefileFeature> features = new CopyOnWriteArrayList<>();
      reader.readFeatures(features::add);
      assertEquals(List.of(1, 2, 3), values(features));
      var tooShort = features.get(2);
      assertThrows(GeometryException.class, tooShort::latLonGeometry);
    }
  }

  @Test
  void testStopsAtTruncatedRecordHeader(@TempDir Path dir) throws IOException {
    var shpPath = writePoints(dir.resolve("test.shp"), 3);
    byte[] shp = Files.readAllBytes(shpPath);
    // keep the first 4 of the last record's 8 header bytes, its content is 20 bytes
    Files.write(shpPath, Arrays.copyOf(shp, shp.length - 20 - 4));
    assertEquals(List.of(1, 2), values(readAll(shpPath, null)));
  }

  @Test
  void testFailsWithoutShpHeader(@TempDir Path dir) throws IOException {
    var shpPath = writePoints(dir.resolve("test.shp"), 1);
    Files.write(shpPath, Arrays.copyOf(Files.readAllBytes(shpPath), 50));
    try (var reader = new ShapefileReader(null, "test", shpPath)) {
      assertThrows(FileFormatException.class, () -> reader.readFeatures(feature -> {
      }));
    }
  }

  /** Writes points with {@code value} 1 to {@code count} in order, the i-th at (i, i). */
  private static Path writePoints(Path shpPath, int count) throws IOException {
    List<Geometry> points = new ArrayList<>();
    for (int i = 1; i <= count; i++) {
      points.add(newPoint(i, i));
    }
    return TestShapefileWriter.write(shpPath, points);
  }

  private static void setSecondPointShapeType(Path shpPath, int shapeTypeId) throws IOException {
    byte[] shp = Files.readAllBytes(shpPath);
    // point records are 28 bytes: the record header, then the shape type, x and y
    int secondRecordContent = ShpHeader.BYTES + 28 + ShpRecordHeader.BYTES;
    ByteBuffer.wrap(shp).order(ByteOrder.LITTLE_ENDIAN).putInt(secondRecordContent, shapeTypeId);
    Files.write(shpPath, shp);
  }

  /** Cuts the last record down to its first {@code contentBytes} and updates the record and file lengths to match. */
  private static void shortenLastRecord(Path shpPath, int contentBytes) throws IOException {
    var shp = ByteBuffer.wrap(Files.readAllBytes(shpPath));
    // lengths in the file and record headers are big endian 16-bit word counts
    int lastRecord = ShpHeader.BYTES;
    for (int next = lastRecord; next < shp.capacity(); next += ShpRecordHeader.BYTES + shp.getInt(next + 4) * 2) {
      lastRecord = next;
    }
    int fileBytes = lastRecord + ShpRecordHeader.BYTES + contentBytes;
    shp.putInt(lastRecord + 4, contentBytes / 2).putInt(24, fileBytes / 2);
    Files.write(shpPath, Arrays.copyOf(shp.array(), fileBytes));
  }

  private static List<Object> values(List<ShapefileFeature> features) {
    return features.stream().map(feature -> feature.getTag("value")).toList();
  }
}
