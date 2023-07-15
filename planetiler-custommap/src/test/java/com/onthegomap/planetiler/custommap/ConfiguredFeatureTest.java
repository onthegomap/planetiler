package com.onthegomap.planetiler.custommap;

import static com.onthegomap.planetiler.TestUtils.newLineString;
import static com.onthegomap.planetiler.TestUtils.newPoint;
import static com.onthegomap.planetiler.TestUtils.newPolygon;
import static java.util.Collections.emptyList;
import static org.junit.jupiter.api.Assertions.*;

import com.onthegomap.planetiler.FeatureCollector;
import com.onthegomap.planetiler.FeatureCollector.Feature;
import com.onthegomap.planetiler.Profile;
import com.onthegomap.planetiler.VectorTile;
import com.onthegomap.planetiler.config.Arguments;
import com.onthegomap.planetiler.config.PlanetilerConfig;
import com.onthegomap.planetiler.custommap.configschema.DataSourceType;
import com.onthegomap.planetiler.custommap.configschema.MergeLineStrings;
import com.onthegomap.planetiler.custommap.configschema.MergeOverlappingPolygons;
import com.onthegomap.planetiler.custommap.configschema.PostProcess;
import com.onthegomap.planetiler.custommap.configschema.SchemaConfig;
import com.onthegomap.planetiler.custommap.util.TestConfigurableUtils;
import com.onthegomap.planetiler.geo.GeoUtils;
import com.onthegomap.planetiler.geo.GeometryException;
import com.onthegomap.planetiler.reader.SimpleFeature;
import com.onthegomap.planetiler.reader.SourceFeature;
import com.onthegomap.planetiler.stats.Stats;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

class ConfiguredFeatureTest {
  private PlanetilerConfig planetilerConfig = PlanetilerConfig.defaults();

  private static final Function<String, Path> TEST_RESOURCE = TestConfigurableUtils::pathToTestResource;
  private static final Function<String, Path> SAMPLE_RESOURCE = TestConfigurableUtils::pathToSample;
  private static final Function<String, Path> TEST_INVALID_RESOURCE = TestConfigurableUtils::pathToTestInvalidResource;

  private static final Map<String, Object> waterTags = Map.of(
    "natural", "water",
    "water", "pond",
    "name", "Little Pond",
    "test_zoom_tag", "test_zoom_value"
  );

  private static final Map<String, Object> motorwayTags = Map.of(
    "highway", "motorway",
    "layer", "1",
    "bridge", "yes",
    "tunnel", "yes"
  );

  private static final Map<String, Object> trunkTags = Map.of(
    "highway", "trunk",
    "toll", "yes"
  );

  private static final Map<String, Object> primaryTags = Map.of(
    "highway", "primary",
    "lanes", "2"

  );

  private static final Map<String, Object> highwayAreaTags = Map.of(
    "area:highway", "motorway",
    "layer", "1",
    "bridge", "yes",
    "surface", "asphalt"
  );

  private static final Map<String, Object> inputMappingTags = Map.of(
    "s_type", "string_val",
    "l_type", "1",
    "i_type", "1",
    "double_type", "1.5",
    "b_type", "yes",
    "d_type", "yes",
    "intermittent", "yes",
    "bridge", "yes"
  );

  private Profile loadConfig(Function<String, Path> pathFunction, String filename) {
    var staticAttributeConfig = pathFunction.apply(filename);
    var schema = SchemaConfig.load(staticAttributeConfig);
    var root = Contexts.buildRootContext(planetilerConfig.arguments(), schema.args());
    planetilerConfig = root.config();
    return new ConfiguredProfile(schema, root);
  }

  private ConfiguredProfile loadConfig(String config) {
    var schema = SchemaConfig.load(config);
    var root = Contexts.buildRootContext(planetilerConfig.arguments(), schema.args());
    planetilerConfig = root.config();
    return new ConfiguredProfile(schema, root);
  }

  private void testFeature(Function<String, Path> pathFunction, String schemaFilename, SourceFeature sf,
    Consumer<Feature> test, int expectedMatchCount) {
    var profile = loadConfig(pathFunction, schemaFilename);
    testFeature(sf, test, expectedMatchCount, profile);
  }

  private void testFeature(String config, SourceFeature sf, Consumer<Feature> test, int expectedMatchCount) {
    var profile = loadConfig(config);
    testFeature(sf, test, expectedMatchCount, profile);
  }


  private void testFeature(SourceFeature sf, Consumer<Feature> test, int expectedMatchCount, Profile profile) {
    var factory = new FeatureCollector.Factory(planetilerConfig, Stats.inMemory());
    var fc = factory.get(sf);

    profile.processFeature(sf, fc);

    var length = new AtomicInteger(0);

    fc.forEach(f -> {
      test.accept(f);
      length.incrementAndGet();
    });

    assertEquals(expectedMatchCount, length.get(), "Wrong number of features generated");
  }

  private void testPolygon(String config, Map<String, Object> tags,
    Consumer<Feature> test, int expectedMatchCount) {
    var sf =
      SimpleFeature.createFakeOsmFeature(newPolygon(0, 0, 1, 0, 1, 1, 0, 0), tags, "osm", null, 1, emptyList());
    testFeature(config, sf, test, expectedMatchCount);
  }

  private void testPoint(String config, Map<String, Object> tags,
    Consumer<Feature> test, int expectedMatchCount) {
    var sf =
      SimpleFeature.createFakeOsmFeature(newPoint(0, 0), tags, "osm", null, 1, emptyList());
    testFeature(config, sf, test, expectedMatchCount);
  }


  private void testLinestring(String config,
    Map<String, Object> tags, Consumer<Feature> test, int expectedMatchCount) {
    var sf =
      SimpleFeature.createFakeOsmFeature(newLineString(0, 0, 1, 0, 1, 1), tags, "osm", null, 1, emptyList());
    testFeature(config, sf, test, expectedMatchCount);
  }

  private void testPolygon(Function<String, Path> pathFunction, String schemaFilename, Map<String, Object> tags,
    Consumer<Feature> test, int expectedMatchCount) {
    var sf =
      SimpleFeature.createFakeOsmFeature(newPolygon(0, 0, 1, 0, 1, 1, 0, 0), tags, "osm", null, 1, emptyList());
    testFeature(pathFunction, schemaFilename, sf, test, expectedMatchCount);
  }

  private void testLinestring(Function<String, Path> pathFunction, String schemaFilename,
    Map<String, Object> tags, Consumer<Feature> test, int expectedMatchCount) {
    var sf =
      SimpleFeature.createFakeOsmFeature(newLineString(0, 0, 1, 0, 1, 1), tags, "osm", null, 1, emptyList());
    testFeature(pathFunction, schemaFilename, sf, test, expectedMatchCount);
  }

  @Test
  void testFeaturePostProcessorNoop() throws GeometryException {
    var config = """
      sources:
        osm:
          type: osm
          url: geofabrik:rhode-island
          local_path: data/rhode-island.osm.pbf
      layers:
      - id: testLayer
        features:
        - source: osm
          geometry: point
      """;
    var profile = loadConfig(config);

    VectorTile.Feature feature = new VectorTile.Feature(
      "testLayer",
      1,
      VectorTile.encodeGeometry(GeoUtils.point(0, 0)),
      Map.of()
    );
    assertEquals(List.of(feature), profile.postProcessLayerFeatures("testLayer", 0, List.of(feature)));
  }

  @Test
  void testFeaturePostProcessorMergeLineStrings() throws GeometryException {
    var config = """
      sources:
        osm:
          type: osm
          url: geofabrik:rhode-island
          local_path: data/rhode-island.osm.pbf
      layers:
      - id: testLayer
        features:
        - source: osm
          geometry: point
        post_process:
          merge_line_strings:
            min_length: 1
            tolerance: 5
            buffer: 10
      """;
    var profile = loadConfig(config);

    VectorTile.Feature feature = new VectorTile.Feature(
      "testLayer",
      1,
      VectorTile.encodeGeometry(GeoUtils.point(0, 0)),
      Map.of()
    );
    assertEquals(List.of(feature), profile.postProcessLayerFeatures("testLayer", 0, List.of(feature)));
  }

  @Test
  void testFeaturePostProcessorMergeOverlappingPolygons() throws GeometryException {
    var config = """
      sources:
        osm:
          type: osm
          url: geofabrik:rhode-island
          local_path: data/rhode-island.osm.pbf
      layers:
      - id: testLayer
        features:
        - source: osm
          geometry: point
        post_process:
          merge_overlapping_polygons:
            min_area: 3
      """;
    var profile = loadConfig(config);

    VectorTile.Feature feature = new VectorTile.Feature(
      "testLayer",
      1,
      VectorTile.encodeGeometry(GeoUtils.point(0, 0)),
      Map.of()
    );
    assertEquals(List.of(feature), profile.postProcessLayerFeatures("testLayer", 0, List.of(feature)));
  }

  @Test
  void testStaticAttributeTest() {
    testPolygon(TEST_RESOURCE, "static_attribute.yml", waterTags, f -> {
      var attr = f.getAttrsAtZoom(14);
      assertEquals("aTestConstantValue", attr.get("natural"));
    }, 1);
  }

  @Test
  void testTagValueAttributeTest() {
    testPolygon(TEST_RESOURCE, "tag_attribute.yml", waterTags, f -> {
      var attr = f.getAttrsAtZoom(14);
      assertEquals("water", attr.get("natural"));
    }, 1);
  }

  @Test
  void testTagIncludeAttributeTest() {
    testPolygon(TEST_RESOURCE, "tag_include.yml", waterTags, f -> {
      var attr = f.getAttrsAtZoom(14);
      assertEquals("ok", attr.get("test_include"));
      assertFalse(attr.containsKey("test_exclude"));
    }, 1);
  }

  @Test
  void testZoomAttributeTest() {
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
  void testTagHighwayLinestringTest() {
    testLinestring(TEST_RESOURCE, "road_motorway.yml", motorwayTags, f -> {
      var attr = f.getAttrsAtZoom(14);
      assertEquals("motorway", attr.get("highway"));
    }, 1);
  }

  @Test
  void testTagTypeConversionTest() {
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
  void testZoomFilterAttributeTest() {
    testLinestring(TEST_RESOURCE, "road_motorway.yml", motorwayTags, f -> {
      var attr = f.getAttrsAtZoom(14);
      assertTrue(attr.containsKey("bridge"), "Produce attribute bridge at z14");

      attr = f.getAttrsAtZoom(10);
      assertFalse(attr.containsKey("bridge"), "Don't produce attribute bridge at z10");
    }, 1);
  }

  @Test
  void testZoomFilterConditionalTest() {
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
  void testAllValuesInKey() {
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
  void testInputMapping() {
    //Show that a key in includeWhen with no values matches all values
    testLinestring(TEST_RESOURCE, "data_type_attributes.yml", inputMappingTags, f -> {
      var attr = f.getAttrsAtZoom(14);
      assertEquals(true, attr.get("b_type"), "Produce boolean");
      assertEquals("string_val", attr.get("s_type"), "Produce string");
      assertEquals(1, attr.get("d_type"), "Produce direction");
      assertEquals(1L, attr.get("l_type"), "Produce long");
      assertEquals(1, attr.get("i_type"), "Produce integer");
      assertEquals(1.5, attr.get("double_type"), "Produce double");

      assertEquals("yes", attr.get("intermittent"), "Produce raw attribute");
      assertEquals(true, attr.get("is_intermittent"), "Produce and rename boolean");
      assertEquals(true, attr.get("bridge"), "Produce boolean from full structure");
    }, 1);
  }

  @ParameterizedTest
  @ValueSource(strings = {"natural:", "natural: [__any__]", "natural: __any__"})
  void testMatchAny(String filter) {
    testPolygon("""
      sources:
        osm:
          type: osm
          url: geofabrik:rhode-island
          local_path: data/rhode-island.osm.pbf
      layers:
      - id: testLayer
        features:
        - source: osm
          geometry: polygon
          include_when:
            %s
      """.formatted(filter), Map.of(
      "natural", "water"
    ), feature -> {
    }, 1);
  }

  @Test
  void testExcludeValue() {
    var config = """
      sources:
        osm:
          type: osm
          url: geofabrik:rhode-island
          local_path: data/rhode-island.osm.pbf
      layers:
      - id: testLayer
        features:
        - source: osm
          geometry: polygon
          include_when:
            natural: water
          exclude_when:
            name: excluded
      """;
    testPolygon(config, Map.of(
      "natural", "water",
      "name", "name"
    ), feature -> {
    }, 1);
    testPolygon(config, Map.of(
      "natural", "water",
      "name", "excluded"
    ), feature -> {
    }, 0);
  }

  @ParameterizedTest
  @ValueSource(strings = {"''", "['']", "[null]"})
  void testRequireValue(String matchString) {
    var config = """
      sources:
        osm:
          type: osm
          url: geofabrik:rhode-island
          local_path: data/rhode-island.osm.pbf
      layers:
      - id: testLayer
        features:
        - source: osm
          geometry: polygon
          include_when:
            natural: water
          exclude_when:
            name: %s
      """.formatted(matchString);
    testPolygon(config, Map.of(
      "natural", "water",
      "name", "name"
    ), feature -> {
    }, 1);
    testPolygon(config, Map.of(
      "natural", "water"
    ), feature -> {
    }, 0);
    testPolygon(config, Map.of(
      "natural", "water",
      "name", ""
    ), feature -> {
    }, 0);
  }

  @Test
  void testMappingKeyValue() {
    testPolygon("""
      sources:
        osm:
          type: osm
          url: geofabrik:rhode-island
          local_path: data/rhode-island.osm.pbf
      layers:
      - id: testLayer
        features:
        - source: osm
          geometry: polygon
          include_when:
            natural: water
          attributes:
          - key: key
            type: match_key
          - key: value
            type: match_value
      """, Map.of(
      "natural", "water"
    ), feature -> {
      assertEquals(Map.of(
        "key", "natural",
        "value", "water"
      ), feature.getAttrsAtZoom(14));
    }, 1);
  }

  @Test
  void testCoerceAttributeValue() {
    testPolygon("""
      sources:
        osm:
          type: osm
          url: geofabrik:rhode-island
          local_path: data/rhode-island.osm.pbf
      layers:
      - id: testLayer
        features:
        - source: osm
          geometry: polygon
          attributes:
          - key: int
            type: integer
          - key: long
            type: long
          - key: double
            type: double
      """, Map.of(
      "int", "1",
      "long", "-1",
      "double", "1.5"
    ), feature -> {
      assertEquals(Map.of(
        "int", 1,
        "long", -1L,
        "double", 1.5
      ), feature.getAttrsAtZoom(14));
    }, 1);
  }

  @ParameterizedTest
  @CsvSource(value = {
    "1| 1",
    "1+1| 1+1",
    "${1+1}| 2",
    "${match_key + '=' + match_value}| natural=water",
    "${match_value.replace('ter', 'wa')}| wawa",
    "${feature.tags.natural}| water",
    "${feature.id}|1",
    "\\${feature.id}|${feature.id}",
    "\\\\${feature.id}|\\${feature.id}",
    "${feature.source}|osm",
    "${feature.source_layer}|null",
    "${coalesce(feature.source_layer, 'missing')}|missing",
    "{match: {test: {natural: water}}}|test",
    "{match: {test: {natural: not_water}}}|null",
  }, delimiter = '|')
  void testExpressionValue(String expression, Object value) {
    testPoint("""
      sources:
        osm:
          type: osm
          url: geofabrik:rhode-island
          local_path: data/rhode-island.osm.pbf
      layers:
      - id: testLayer
        features:
        - source: osm
          geometry: point
          include_when:
            natural: water
          attributes:
          - key: key
            value: %s
      """.formatted(expression), Map.of(
      "natural", "water"
    ), feature -> {
      var result = feature.getAttrsAtZoom(14).get("key");
      String resultString = result == null ? "null" : result.toString();
      assertEquals(value, resultString);
    }, 1);
  }

  @Test
  void testGetTag() {
    testPoint("""
      sources:
        osm:
          type: osm
          url: geofabrik:rhode-island
          local_path: data/rhode-island.osm.pbf
      layers:
      - id: testLayer
        features:
        - source: osm
          geometry: point
          include_when:
            natural: water
          attributes:
          - key: key
            value:
              tag_value: natural
          - key: key2
            value:
              tag_value: intval
              type: integer
      """, Map.of(
      "natural", "water",
      "intval", "1"
    ), feature -> {
      assertEquals("water", feature.getAttrsAtZoom(14).get("key"));
      assertEquals(1, feature.getAttrsAtZoom(14).get("key2"));
    }, 1);
  }

  @ParameterizedTest
  @ValueSource(strings = {
    "",
    "tag_value: depth",
    "value: '${feature.tags[\"depth\"]}'",
    "value: '${feature.tags.get(\"depth\")}'"
  })
  void testGetInExpressionUsesTagMapping(String getter) {
    testPoint("""
      sources:
        osm:
          type: osm
          url: geofabrik:rhode-island
          local_path: data/rhode-island.osm.pbf
      tag_mappings:
        depth: long
      layers:
      - id: testLayer
        features:
        - source: osm
          geometry: point
          attributes:
          - key: depth
            %s
      """.formatted(getter), Map.of(
      "depth", "35"
    ), feature -> {
      assertEquals(35L, feature.getAttrsAtZoom(14).get("depth"));
    }, 1);
  }

  @ParameterizedTest
  @CsvSource(value = {
    "12|12",
    "${5+5}|10",
    "${match_key.size()}|7",
    "${value.size()}|5",
    "{default_value: 4, overrides: {3: {natural: water}}}|3",
    "{default_value: 4, overrides: {3: {natural: not_water}}}|4",
  }, delimiter = '|')
  void testAttributeMinZoomExpression(String expression, int minZoom) {
    testPoint("""
      sources:
        osm:
          type: osm
          url: geofabrik:rhode-island
          local_path: data/rhode-island.osm.pbf
      layers:
      - id: testLayer
        features:
        - source: osm
          geometry: point
          include_when:
            natural: water
          attributes:
          - key: key
            value: value
            min_zoom: %s
      """.formatted(expression), Map.of(
      "natural", "water"
    ), feature -> {
      assertNull(feature.getAttrsAtZoom(minZoom - 1).get("key"));
      assertEquals("value", feature.getAttrsAtZoom(minZoom).get("key"));
    }, 1);
  }

  @Test
  void testMinZoomExpression() {
    var config = """
      sources:
        osm:
          type: osm
          url: geofabrik:rhode-island
          local_path: data/rhode-island.osm.pbf
      layers:
      - id: testLayer
        features:
        - source: osm
          geometry: point
          min_zoom:
            default_value: 4
            overrides:
            - if: '${feature.tags.has("a", "b")}'
              value: 5
          include_when:
            natural: water
      """;
    testPoint(config, Map.of(
      "natural", "water"
    ), feature -> {
      assertEquals(4, feature.getMinZoom());
    }, 1);
    testPoint(config, Map.of(
      "natural", "water",
      "a", "b"
    ), feature -> {
      assertEquals(5, feature.getMinZoom());
    }, 1);
  }

  @Test
  void testFallbackValue() {
    var config = """
      sources:
        osm:
          type: osm
          url: geofabrik:rhode-island
          local_path: data/rhode-island.osm.pbf
      layers:
      - id: testLayer
        features:
        - source: osm
          geometry: polygon
          include_when:
            natural: water
          attributes:
          - key: key
            value: 1
            include_when:
              otherkey: value
            else: 0
      """;
    testPolygon(config, Map.of(
      "natural", "water"
    ), feature -> {
      assertEquals(Map.of("key", 0), feature.getAttrsAtZoom(14));
    }, 1);
    testPolygon(config, Map.of(
      "natural", "water",
      "otherkey", "othervalue"
    ), feature -> {
      assertEquals(Map.of("key", 0), feature.getAttrsAtZoom(14));
    }, 1);
    testPolygon(config, Map.of(
      "natural", "water",
      "otherkey", "value"
    ), feature -> {
      assertEquals(Map.of("key", 1), feature.getAttrsAtZoom(14));
    }, 1);
  }

  @ParameterizedTest
  @CsvSource(value = {
    "\"${feature.tags.has('natural', 'water')}\"",
    "{__all__: [\"${feature.tags.has('natural', 'water')}\"]}",
  }, delimiter = '|')
  void testExpressionInMatch(String filter) {
    var config = """
      sources:
        osm:
          type: osm
          url: geofabrik:rhode-island
          local_path: data/rhode-island.osm.pbf
      layers:
      - id: testLayer
        features:
        - source: osm
          geometry: polygon
          include_when: %s
      """.formatted(filter);

    testPolygon(config, Map.of(
      "natural", "water"
    ), feature -> {
    }, 1);

    testPolygon(config, Map.of(
      "natural", "other"
    ), feature -> {
    }, 0);

    testPolygon(config, Map.of(
    ), feature -> {
    }, 0);
  }

  @Test
  void testExpressionAttrFilter() {
    var config = """
      sources:
        osm:
          type: osm
          url: geofabrik:rhode-island
          local_path: data/rhode-island.osm.pbf
      layers:
      - id: testLayer
        features:
        - source: osm
          geometry: polygon
          include_when:
            natural: water
            highway: motorway
          attributes:
          - key: key
            value: true
            include_when: ${ match_value.startsWith("wa") }
            else: false
      """;

    testPolygon(config, Map.of(
      "natural", "water"
    ), feature -> {
      assertEquals(true, feature.getAttrsAtZoom(14).get("key"));
    }, 1);

    testPolygon(config, Map.of(
      "highway", "motorway"
    ), feature -> {
      assertEquals(false, feature.getAttrsAtZoom(14).get("key"));
    }, 1);
  }

  @Test
  void testExpressionAttrFilterNoMatchingKey() {
    var config = """
      sources:
        osm:
          type: osm
          url: geofabrik:rhode-island
          local_path: data/rhode-island.osm.pbf
      layers:
      - id: testLayer
        features:
        - source: osm
          geometry: polygon
          include_when: ${ feature.tags.has("natural", "water") }
          attributes:
          - key: key
            value: true
            include_when: ${ coalesce(match_value, '').startsWith("wa") }
            else: false
      """;

    testPolygon(config, Map.of(
      "natural", "water"
    ), feature -> {
      assertEquals(false, feature.getAttrsAtZoom(14).get("key"));
    }, 1);
  }

  @Test
  void testGeometryTypeMismatch() {
    //Validate that a schema that filters on lines does not match on a polygon feature
    var sf =
      SimpleFeature.createFakeOsmFeature(newPolygon(0, 0, 1, 0, 1, 1, 0, 0), motorwayTags, "osm", null, 1,
        emptyList());

    testFeature(TEST_RESOURCE, "road_motorway.yml", sf, f -> {
    }, 0);
  }

  @Test
  void testSourceTypeMismatch() {
    //Validate that a schema only matches on the specified data source
    var sf =
      SimpleFeature.createFakeOsmFeature(newLineString(0, 0, 1, 0, 1, 1, 0, 0), highwayAreaTags, "not_osm", null, 1,
        emptyList());

    testFeature(SAMPLE_RESOURCE, "highway_areas.yml", sf, f -> {
    }, 0);
  }

  @Test
  void testInvalidSchemas() {
    testInvalidSchema("bad_geometry_type.yml", "Profile defined with invalid geometry type");
    testInvalidSchema("no_layers.yml", "Profile defined with no layers");
    testInvalidSchema("invalid_post_process.yml", "Profile defined with invalid post process element");
  }

  private void testInvalidSchema(String filename, String message) {
    assertThrows(RuntimeException.class, () -> loadConfig(TEST_INVALID_RESOURCE, filename), message);
  }

  @ParameterizedTest
  @ValueSource(strings = {
    "arg_value: argument",
    "value: '${ args.argument }'",
    "value: '${ args[\"argument\"] }'",
  })
  void testUseArgumentNotDefined(String string) {
    this.planetilerConfig = PlanetilerConfig.from(Arguments.of(Map.of(
      "argument", "value"
    )));
    var config = """
      sources:
        osm:
          type: osm
          url: geofabrik:rhode-island
          local_path: data/rhode-island.osm.pbf
      layers:
      - id: testLayer
        features:
        - source: osm
          geometry: point
          include_when:
            natural: water
          attributes:
          - key: key
            %s
      """.formatted(string);

    testPoint(config, Map.of(
      "natural", "water"
    ), feature -> {
      assertEquals("value", feature.getAttrsAtZoom(14).get("key"));
    }, 1);
  }

  @ParameterizedTest
  @ValueSource(strings = {
    "arg_value: threads",
    "value: '${ args.threads }'",
    "value: '${ args[\"threads\"] }'",
  })
  void testOverrideArgument(String string) {
    var config = """
      sources:
        osm:
          type: osm
          url: geofabrik:rhode-island
          local_path: data/rhode-island.osm.pbf
      args:
        threads: 2
      layers:
      - id: testLayer
        features:
        - source: osm
          geometry: point
          include_when:
            natural: water
          attributes:
          - key: key
            type: integer
            %s
      """.formatted(string);

    testPoint(config, Map.of(
      "natural", "water"
    ), feature -> {
      assertEquals(2, feature.getAttrsAtZoom(14).get("key"));
    }, 1);
  }

  @Test
  void testDefineArgument() {
    this.planetilerConfig = PlanetilerConfig.from(Arguments.of(Map.of(
      "custom_overridden_arg", "test2",
      "custom_simple_overridden_int_arg", "3"
    )));
    var config = """
      sources:
        osm:
          type: osm
          url: geofabrik:rhode-island
          local_path: data/rhode-island.osm.pbf
      args:
        custom_int_arg:
          type: integer
          description: test arg out
          default: 12
        custom_boolean_arg:
          type: boolean
          description: test boolean arg out
          default: true
        custom_overridden_arg:
          default: test
        custom_simple_string_arg: value
        custom_simple_int_arg: 1
        custom_simple_double_arg: 1.5
        custom_simple_bool_arg: true
        custom_simple_overridden_int_arg: 2
      layers:
      - id: testLayer
        features:
        - source: osm
          geometry: point
          include_when:
            natural: water
          attributes:
          - key: int
            value: '${ args.custom_int_arg }'
          - key: bool
            value: '${ args["custom_boolean_arg"] }'
          - key: overridden
            arg_value: custom_overridden_arg
          - key: custom_simple_string_arg
            arg_value: custom_simple_string_arg
          - key: custom_simple_int_arg
            arg_value: custom_simple_int_arg
          - key: custom_simple_bool_arg
            arg_value: custom_simple_bool_arg
          - key: custom_simple_overridden_int_arg
            arg_value: custom_simple_overridden_int_arg
          - key: custom_simple_double_arg
            arg_value: custom_simple_double_arg
          - key: custom_simple_int_arg_as_string
            arg_value: custom_simple_int_arg
            type: string
      """;

    testPoint(config, Map.of(
      "natural", "water"
    ), feature -> {
      assertEquals(12L, feature.getAttrsAtZoom(14).get("int"));
      assertEquals(true, feature.getAttrsAtZoom(14).get("bool"));
      assertEquals("test2", feature.getAttrsAtZoom(14).get("overridden"));

      assertEquals("value", feature.getAttrsAtZoom(14).get("custom_simple_string_arg"));
      assertEquals(1, feature.getAttrsAtZoom(14).get("custom_simple_int_arg"));
      assertEquals("1", feature.getAttrsAtZoom(14).get("custom_simple_int_arg_as_string"));
      assertEquals(1.5, feature.getAttrsAtZoom(14).get("custom_simple_double_arg"));
      assertEquals(true, feature.getAttrsAtZoom(14).get("custom_simple_bool_arg"));
      assertEquals(3, feature.getAttrsAtZoom(14).get("custom_simple_overridden_int_arg"));
    }, 1);
  }

  @Test
  void testDefineArgumentsUsingExpressions() {
    this.planetilerConfig = PlanetilerConfig.from(Arguments.of(Map.of(
      "custom_overridden_arg", "test2",
      "custom_simple_overridden_int_arg", "3"
    )));
    var config = """
      sources:
        osm:
          type: osm
          url: geofabrik:rhode-island
          local_path: data/rhode-island.osm.pbf
      args:
        arg1:
          type: long
          description: test arg out
          default: '${ 2 - 1 }'
        arg2: '${ 2 - 1 }'
        arg3:
          default: '${ 2 - 1 }'
        arg4: ${ args.arg3 + 1 }
      layers:
      - id: testLayer
        features:
        - source: osm
          geometry: point
          attributes:
          - key: arg1
            arg_value: arg1
          - key: arg2
            arg_value: arg2
          - key: arg3
            arg_value: arg3
          - key: arg4
            arg_value: arg4
      """;

    testPoint(config, Map.of(), feature -> {
      assertEquals(1L, feature.getAttrsAtZoom(14).get("arg1"));
      assertEquals(1L, feature.getAttrsAtZoom(14).get("arg2"));
      assertEquals(1L, feature.getAttrsAtZoom(14).get("arg3"));
      assertEquals(2L, feature.getAttrsAtZoom(14).get("arg4"));
    }, 1);
  }

  @Test
  void testUseArgumentInSourceUrlPath() {
    var config = """
      args:
        area: rhode-island
        url: '${ "geofabrik:" + args.area }'
      sources:
        osm:
          type: osm
          url: '${ args.url }'
      layers:
      - id: testLayer
        features:
        - source: osm
          geometry: point
      """;
    this.planetilerConfig = PlanetilerConfig.from(Arguments.of(Map.of(
      "area", "boston"
    )));
    assertEquals(List.of(new Source(
      "osm",
      DataSourceType.OSM,
      "geofabrik:boston",
      null
    )), loadConfig(config).sources());

    this.planetilerConfig = PlanetilerConfig.from(Arguments.of(Map.of()));
    assertEquals(List.of(new Source(
      "osm",
      DataSourceType.OSM,
      "geofabrik:rhode-island",
      null
    )), loadConfig(config).sources());

    this.planetilerConfig = PlanetilerConfig.from(Arguments.of(Map.of()));
    assertEquals("geofabrik_rhode_island.osm.pbf", loadConfig(config).sources().get(0).defaultFileUrl());

    this.planetilerConfig = PlanetilerConfig.from(Arguments.of(Map.of(
      "url", "https://example.com/file.osm.pbf"
    )));
    assertEquals("example.com_file.osm.pbf", loadConfig(config).sources().get(0).defaultFileUrl());
  }

  @Test
  void testSchemaEmptyPostProcess() {
    var config = """
      sources:
        osm:
          type: osm
          url: geofabrik:rhode-island
          local_path: data/rhode-island.osm.pbf
      layers:
      - id: testLayer
        features:
        - source: osm
          geometry: point
      """;
    this.planetilerConfig = PlanetilerConfig.from(Arguments.of(Map.of()));
    assertEquals(null, loadConfig(config).findFeatureLayer("testLayer").postProcess());
  }

  @Test
  void testSchemaPostProcessWithMergeLineStrings() {
    var config = """
      sources:
        osm:
          type: osm
          url: geofabrik:rhode-island
          local_path: data/rhode-island.osm.pbf
      layers:
      - id: testLayer
        features:
        - source: osm
          geometry: point
        post_process:
          merge_line_strings:
            min_length: 1
            tolerance: 5
            buffer: 10
      """;
    this.planetilerConfig = PlanetilerConfig.from(Arguments.of(Map.of()));
    assertEquals(new PostProcess(
      new MergeLineStrings(
        1,
        5,
        10
      ),
      null
    ), loadConfig(config).findFeatureLayer("testLayer").postProcess());
  }

  @Test
  void testSchemaPostProcessWithMergeOverlappingPolygons() {
    var config = """
      sources:
        osm:
          type: osm
          url: geofabrik:rhode-island
          local_path: data/rhode-island.osm.pbf
      layers:
      - id: testLayer
        features:
        - source: osm
          geometry: point
        post_process:
          merge_overlapping_polygons:
            min_area: 3
      """;
    this.planetilerConfig = PlanetilerConfig.from(Arguments.of(Map.of()));
    assertEquals(new PostProcess(
      null,
      new MergeOverlappingPolygons(
        3
      )
    ), loadConfig(config).findFeatureLayer("testLayer").postProcess());
  }
}
