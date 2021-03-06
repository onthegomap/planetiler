package com.onthegomap.planetiler.custommap;

import static com.onthegomap.planetiler.TestUtils.newLineString;
import static com.onthegomap.planetiler.TestUtils.newPolygon;
import static java.util.Collections.emptyList;
import static org.junit.jupiter.api.Assertions.*;

import com.onthegomap.planetiler.FeatureCollector;
import com.onthegomap.planetiler.FeatureCollector.Feature;
import com.onthegomap.planetiler.Profile;
import com.onthegomap.planetiler.TestUtils;
import com.onthegomap.planetiler.config.PlanetilerConfig;
import com.onthegomap.planetiler.custommap.util.TestConfigurableUtils;
import com.onthegomap.planetiler.reader.SimpleFeature;
import com.onthegomap.planetiler.reader.SourceFeature;
import com.onthegomap.planetiler.stats.Stats;
import java.io.IOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import org.junit.jupiter.api.Test;

class ConfiguredFeatureTest {

  private static final Function<String, Path> TEST_RESOURCE = TestConfigurableUtils::pathToTestResource;
  private static final Function<String, Path> SAMPLE_RESOURCE = TestConfigurableUtils::pathToSample;
  private static final Function<String, Path> TEST_INVALID_RESOURCE = TestConfigurableUtils::pathToTestInvalidResource;

  private static final Map<String, Object> waterTags = Map.of(
    "natural", "water",
    "water", "pond",
    "name", "Little Pond",
    "test_zoom_tag", "test_zoom_value"
  );

  private static Map<String, Object> motorwayTags = Map.of(
    "highway", "motorway",
    "layer", "1",
    "bridge", "yes",
    "tunnel", "yes"
  );

  private static Map<String, Object> trunkTags = Map.of(
    "highway", "trunk",
    "toll", "yes"
  );

  private static Map<String, Object> primaryTags = Map.of(
    "highway", "primary",
    "lanes", "2"

  );

  private static Map<String, Object> highwayAreaTags = Map.of(
    "area:highway", "motorway",
    "layer", "1",
    "bridge", "yes",
    "surface", "asphalt"
  );

  private static Map<String, Object> inputMappingTags = Map.of(
    "s_type", "string_val",
    "l_type", "1",
    "b_type", "yes",
    "d_type", "yes",
    "intermittent", "yes",
    "bridge", "yes"
  );

  private static FeatureCollector polygonFeatureCollector() {
    var config = PlanetilerConfig.defaults();
    var factory = new FeatureCollector.Factory(config, Stats.inMemory());
    return factory.get(SimpleFeature.create(TestUtils.newPolygon(0, 0, 0.1, 0, 0.1, 0.1, 0, 0), new HashMap<>()));
  }

  private static FeatureCollector linestringFeatureCollector() {
    var config = PlanetilerConfig.defaults();
    var factory = new FeatureCollector.Factory(config, Stats.inMemory());
    return factory.get(SimpleFeature.create(TestUtils.newLineString(0, 0, 0.1, 0, 0.1, 0.1, 0, 0), new HashMap<>()));
  }

  private static Profile loadConfig(Function<String, Path> pathFunction, String filename) throws IOException {
    var staticAttributeConfig = pathFunction.apply(filename);
    var schema = ConfiguredMapMain.loadConfig(staticAttributeConfig);
    return new ConfiguredProfile(schema);
  }

  private static void testFeature(Function<String, Path> pathFunction, String schemaFilename, SourceFeature sf,
    Supplier<FeatureCollector> fcFactory,
    Consumer<Feature> test, int expectedMatchCount)
    throws Exception {

    var profile = loadConfig(pathFunction, schemaFilename);
    var fc = fcFactory.get();

    profile.processFeature(sf, fc);

    var length = new AtomicInteger(0);

    fc.forEach(f -> {
      test.accept(f);
      length.incrementAndGet();
    });

    assertEquals(expectedMatchCount, length.get(), "Wrong number of features generated");
  }

  private static void testPolygon(Function<String, Path> pathFunction, String schemaFilename, Map<String, Object> tags,
    Consumer<Feature> test, int expectedMatchCount)
    throws Exception {
    var sf =
      SimpleFeature.createFakeOsmFeature(newPolygon(0, 0, 1, 0, 1, 1, 0, 0), tags, "osm", null, 1, emptyList());
    testFeature(pathFunction, schemaFilename, sf,
      ConfiguredFeatureTest::polygonFeatureCollector, test, expectedMatchCount);
  }

  private static void testLinestring(Function<String, Path> pathFunction, String schemaFilename,
    Map<String, Object> tags, Consumer<Feature> test, int expectedMatchCount)
    throws Exception {
    var sf =
      SimpleFeature.createFakeOsmFeature(newLineString(0, 0, 1, 0, 1, 1), tags, "osm", null, 1, emptyList());
    testFeature(pathFunction, schemaFilename, sf,
      ConfiguredFeatureTest::linestringFeatureCollector, test, expectedMatchCount);
  }

  @Test
  void testStaticAttributeTest() throws Exception {
    testPolygon(TEST_RESOURCE, "static_attribute.yml", waterTags, f -> {
      var attr = f.getAttrsAtZoom(14);
      assertEquals("aTestConstantValue", attr.get("natural"));
    }, 1);
  }

  @Test
  void testTagValueAttributeTest() throws Exception {
    testPolygon(TEST_RESOURCE, "tag_attribute.yml", waterTags, f -> {
      var attr = f.getAttrsAtZoom(14);
      assertEquals("water", attr.get("natural"));
    }, 1);
  }

  @Test
  void testTagIncludeAttributeTest() throws Exception {
    testPolygon(TEST_RESOURCE, "tag_include.yml", waterTags, f -> {
      var attr = f.getAttrsAtZoom(14);
      assertEquals("ok", attr.get("test_include"));
      assertFalse(attr.containsKey("test_exclude"));
    }, 1);
  }

  @Test
  void testZoomAttributeTest() throws Exception {
    testPolygon(TEST_RESOURCE, "tag_include.yml", waterTags, f -> {
      var attr = f.getAttrsAtZoom(14);
      assertEquals("test_zoom_value", attr.get("test_zoom_tag"));

      attr = f.getAttrsAtZoom(11);
      assertNotEquals("test_zoom_value", attr.get("test_zoom_tag"));

      attr = f.getAttrsAtZoom(9);
      assertNotEquals("test_zoom_value", attr.get("test_zoom_tag"));
    }, 1);
  }

  @Test
  void testTagHighwayLinestringTest() throws Exception {
    testLinestring(TEST_RESOURCE, "road_motorway.yml", motorwayTags, f -> {
      var attr = f.getAttrsAtZoom(14);
      assertEquals("motorway", attr.get("highway"));
    }, 1);
  }

  @Test
  void testTagTypeConversionTest() throws Exception {
    testLinestring(TEST_RESOURCE, "road_motorway.yml", motorwayTags, f -> {
      var attr = f.getAttrsAtZoom(14);

      assertTrue(attr.containsKey("layer"), "Produce attribute layer");
      assertTrue(attr.containsKey("bridge"), "Produce attribute bridge");
      assertTrue(attr.containsKey("tunnel"), "Produce attribute tunnel");

      assertEquals(1L, attr.get("layer"), "Extract layer as LONG");
      assertEquals(true, attr.get("bridge"), "Extract bridge as tagValue BOOLEAN");
      assertEquals(true, attr.get("tunnel"), "Extract tunnel as constantValue BOOLEAN");
    }, 1);
  }

  @Test
  void testZoomFilterAttributeTest() throws Exception {
    testLinestring(TEST_RESOURCE, "road_motorway.yml", motorwayTags, f -> {
      var attr = f.getAttrsAtZoom(14);
      assertTrue(attr.containsKey("bridge"), "Produce attribute bridge at z14");

      attr = f.getAttrsAtZoom(10);
      assertFalse(attr.containsKey("bridge"), "Don't produce attribute bridge at z10");
    }, 1);
  }

  @Test
  void testZoomFilterConditionalTest() throws Exception {
    testLinestring(TEST_RESOURCE, "zoom_filter.yml", motorwayTags, f -> {
      var attr = f.getAttrsAtZoom(4);
      assertEquals("motorway", attr.get("highway"), "Produce attribute highway at z4");
    }, 1);

    testLinestring(TEST_RESOURCE, "zoom_filter.yml", trunkTags, f -> {
      assertEquals(5, f.getMinZoom());
      var attr = f.getAttrsAtZoom(5);
      assertEquals("trunk", attr.get("highway"), "Produce highway=trunk at z5");
      assertNull(attr.get("toll"), "Skip toll at z5");

      attr = f.getAttrsAtZoom(6);
      assertEquals("trunk", attr.get("highway"), "Produce highway=trunk at z6");

      attr = f.getAttrsAtZoom(8);
      assertEquals("yes", attr.get("toll"), "render toll at z8");
    }, 1);

    testLinestring(TEST_RESOURCE, "zoom_filter.yml", primaryTags, f -> {
      var attr = f.getAttrsAtZoom(6);
      assertNull(attr.get("highway"), "Skip highway=primary at z6");
      assertNull(attr.get("lanes"));

      attr = f.getAttrsAtZoom(7);
      assertEquals("primary", attr.get("highway"), "Produce highway=primary at z7");
      assertNull(attr.get("lanes"));

      attr = f.getAttrsAtZoom(12);
      assertEquals("primary", attr.get("highway"), "Produce highway=primary at z12");
      assertEquals(2L, attr.get("lanes"));
    }, 1);
  }

  @Test
  void testAllValuesInKey() throws Exception {
    //Show that a key in includeWhen with no values matches all values
    testPolygon(SAMPLE_RESOURCE, "highway_areas.yml", highwayAreaTags, f -> {
      var attr = f.getAttrsAtZoom(14);
      assertEquals(true, attr.get("bridge"), "Produce bridge attribute");
      assertEquals("motorway", attr.get("highway"), "Produce highway area attribute");
      assertEquals("asphalt", attr.get("surface"), "Produce surface attribute");
      assertEquals(1L, attr.get("layer"), "Produce layer attribute");
    }, 1);
  }

  @Test
  void testInputMapping() throws Exception {
    //Show that a key in includeWhen with no values matches all values
    testLinestring(TEST_RESOURCE, "data_type_attributes.yml", inputMappingTags, f -> {
      var attr = f.getAttrsAtZoom(14);
      assertEquals(true, attr.get("b_type"), "Produce boolean");
      assertEquals("string_val", attr.get("s_type"), "Produce string");
      assertEquals(1, attr.get("d_type"), "Produce direction");
      assertEquals(1L, attr.get("l_type"), "Produce long");

      assertEquals("yes", attr.get("intermittent"), "Produce raw attribute");
      assertEquals(true, attr.get("is_intermittent"), "Produce and rename boolean");
      assertEquals(true, attr.get("bridge"), "Produce boolean from full structure");
    }, 1);
  }

  @Test
  void testGeometryTypeMismatch() throws Exception {
    //Validate that a schema that filters on lines does not match on a polygon feature
    var sf =
      SimpleFeature.createFakeOsmFeature(newPolygon(0, 0, 1, 0, 1, 1, 0, 0), motorwayTags, "osm", null, 1,
        emptyList());

    testFeature(TEST_RESOURCE, "road_motorway.yml", sf,
      ConfiguredFeatureTest::linestringFeatureCollector, f -> {
      }, 0);
  }

  @Test
  void testSourceTypeMismatch() throws Exception {
    //Validate that a schema only matches on the specified data source
    var sf =
      SimpleFeature.createFakeOsmFeature(newLineString(0, 0, 1, 0, 1, 1, 0, 0), highwayAreaTags, "not_osm", null, 1,
        emptyList());

    testFeature(SAMPLE_RESOURCE, "highway_areas.yml", sf,
      ConfiguredFeatureTest::linestringFeatureCollector, f -> {
      }, 0);
  }

  @Test
  void testInvalidSchemas() throws Exception {
    testInvalidSchema("bad_geometry_type.yml", "Profile defined with invalid geometry type");
    testInvalidSchema("no_layers.yml", "Profile defined with no layers");
  }

  private void testInvalidSchema(String filename, String message) {
    assertThrows(RuntimeException.class, () -> loadConfig(TEST_INVALID_RESOURCE, filename), message);
  }
}
