package com.onthegomap.planetiler.custommap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import java.io.FileInputStream;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.yaml.snakeyaml.Yaml;

import com.onthegomap.planetiler.FeatureCollector;
import com.onthegomap.planetiler.FeatureCollector.Feature;
import com.onthegomap.planetiler.Profile;
import com.onthegomap.planetiler.TestUtils;
import com.onthegomap.planetiler.config.PlanetilerConfig;
import com.onthegomap.planetiler.custommap.configschema.SchemaConfig;
import com.onthegomap.planetiler.reader.SimpleFeature;
import com.onthegomap.planetiler.reader.SourceFeature;
import com.onthegomap.planetiler.stats.Stats;

public class ConfiguredSingleFeatureTest {

  private static FeatureCollector polygonFeatureCollector() {
    PlanetilerConfig config = PlanetilerConfig.defaults();
    FeatureCollector.Factory factory = new FeatureCollector.Factory(config, Stats.inMemory());
    return factory.get(SimpleFeature.create(TestUtils.newPolygon(0, 0, 0.1, 0, 0.1, 0.1, 0, 0), new HashMap<>()));
  }

  private static Profile configureProfile(String filename) throws Exception {
    Path staticAttributeConfig = TestConfigurableUtils.pathToResource(filename);
    Yaml yml = new Yaml();
    SchemaConfig schema = yml.loadAs(new FileInputStream(staticAttributeConfig.toFile()), SchemaConfig.class);

    ConfiguredProfile profile = new ConfiguredProfile(schema);
    return profile;
  }

  private static void testPolygon(String profileConfig, Map<String, Object> tags, Consumer<Feature> test)
    throws Exception {
    SourceFeature sf = new TestAreaSourceFeature(tags, "osm", "testLayer");

    Profile profile = configureProfile(profileConfig);
    FeatureCollector fc = polygonFeatureCollector();

    profile.processFeature(sf, fc);

    AtomicInteger length = new AtomicInteger(0);

    fc.forEach(f -> {
      test.accept(f);
      length.incrementAndGet();
    });

    assertEquals(1, length.get());
  }

  private static Map<String, Object> tags = new HashMap<>();

  @BeforeAll
  private static void setup() {
    tags.put("natural", "water");
    tags.put("water", "pond");
    tags.put("name", "Little Pond");
  }

  @Test
  public void testStaticAttributeTest() throws Exception {
    testPolygon("static_attribute.yml", tags, f -> {
      Map<String, Object> attr = f.getAttrsAtZoom(14);
      assertEquals("aTestConstantValue", attr.get("natural"));
    });
  }

  @Test
  public void testTagValueAttributeTest() throws Exception {
    testPolygon("tag_attribute.yml", tags, f -> {
      Map<String, Object> attr = f.getAttrsAtZoom(14);
      assertEquals("water", attr.get("natural"));
    });
  }

  @Test
  public void testTagIncludeAttributeTest() throws Exception {
    testPolygon("tag_include.yml", tags, f -> {
      Map<String, Object> attr = f.getAttrsAtZoom(14);
      assertEquals("ok", attr.get("test_include"));
      assertFalse(attr.containsKey("test_exclude"));
    });
  }

}
