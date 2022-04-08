package com.onthegomap.planetiler.custommap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import com.onthegomap.planetiler.FeatureCollector;
import com.onthegomap.planetiler.FeatureCollector.Feature;
import com.onthegomap.planetiler.Profile;
import com.onthegomap.planetiler.TestUtils;
import com.onthegomap.planetiler.config.PlanetilerConfig;
import com.onthegomap.planetiler.custommap.configschema.SchemaConfig;
import com.onthegomap.planetiler.reader.SimpleFeature;
import com.onthegomap.planetiler.reader.SourceFeature;
import com.onthegomap.planetiler.stats.Stats;
import java.io.FileInputStream;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.yaml.snakeyaml.Yaml;

public class ConfiguredSingleFeatureTest {

  private static FeatureCollector polygonFeatureCollector() {
    PlanetilerConfig config = PlanetilerConfig.defaults();
    FeatureCollector.Factory factory = new FeatureCollector.Factory(config, Stats.inMemory());
    return factory.get(SimpleFeature.create(TestUtils.newPolygon(0, 0, 0.1, 0, 0.1, 0.1, 0, 0), new HashMap<>()));
  }

  private static FeatureCollector linestringFeatureCollector() {
    PlanetilerConfig config = PlanetilerConfig.defaults();
    FeatureCollector.Factory factory = new FeatureCollector.Factory(config, Stats.inMemory());
    return factory.get(SimpleFeature.create(TestUtils.newLineString(0, 0, 0.1, 0, 0.1, 0.1, 0, 0), new HashMap<>()));
  }

  private static Profile configureProfile(String filename) throws Exception {
    Path staticAttributeConfig = TestConfigurableUtils.pathToResource(filename);
    Yaml yml = new Yaml();
    SchemaConfig schema = yml.loadAs(new FileInputStream(staticAttributeConfig.toFile()), SchemaConfig.class);

    ConfiguredProfile profile = new ConfiguredProfile(schema);
    return profile;
  }

  private static void testFeature(String profileConfig, SourceFeature sf, Supplier<FeatureCollector> fcFactory,
    Consumer<Feature> test)
    throws Exception {

    Profile profile = configureProfile(profileConfig);
    FeatureCollector fc = fcFactory.get();

    profile.processFeature(sf, fc);

    AtomicInteger length = new AtomicInteger(0);

    fc.forEach(f -> {
      test.accept(f);
      length.incrementAndGet();
    });

    assertEquals(1, length.get());
  }

  private static void testPolygon(String profileConfig, Map<String, Object> tags, Consumer<Feature> test)
    throws Exception {
    SourceFeature sf = new TestAreaSourceFeature(tags, "osm", "testLayer");
    testFeature(profileConfig, sf, ConfiguredSingleFeatureTest::polygonFeatureCollector, test);
  }

  private static void testLinestring(String profileConfig, Map<String, Object> tags, Consumer<Feature> test)
    throws Exception {
    SourceFeature sf = new TestLinestringSourceFeature(tags, "osm", "testLayer");
    testFeature(profileConfig, sf, ConfiguredSingleFeatureTest::linestringFeatureCollector, test);
  }

  private static Map<String, Object> waterTags = new HashMap<>();
  private static Map<String, Object> motorwayTags = new HashMap<>();

  @BeforeAll
  private static void setup() {
    waterTags.put("natural", "water");
    waterTags.put("water", "pond");
    waterTags.put("name", "Little Pond");

    motorwayTags.put("highway", "motorway");
    motorwayTags.put("layer", "1");
    motorwayTags.put("bridge", "yes");
  }

  @Test
  public void testStaticAttributeTest() throws Exception {
    testPolygon("static_attribute.yml", waterTags, f -> {
      Map<String, Object> attr = f.getAttrsAtZoom(14);
      assertEquals("aTestConstantValue", attr.get("natural"));
    });
  }

  @Test
  public void testTagValueAttributeTest() throws Exception {
    testPolygon("tag_attribute.yml", waterTags, f -> {
      Map<String, Object> attr = f.getAttrsAtZoom(14);
      assertEquals("water", attr.get("natural"));
    });
  }

  @Test
  public void testTagIncludeAttributeTest() throws Exception {
    testPolygon("tag_include.yml", waterTags, f -> {
      Map<String, Object> attr = f.getAttrsAtZoom(14);
      assertEquals("ok", attr.get("test_include"));
      assertFalse(attr.containsKey("test_exclude"));
    });
  }

  @Test
  public void testTagHighwayLinestringTest() throws Exception {
    testLinestring("road_motorway.yml", motorwayTags, f -> {
      Map<String, Object> attr = f.getAttrsAtZoom(14);
      assertEquals("motorway", attr.get("highway"));
    });
  }

}
