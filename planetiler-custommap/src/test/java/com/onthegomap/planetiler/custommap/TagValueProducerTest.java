package com.onthegomap.planetiler.custommap;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.onthegomap.planetiler.geo.GeoUtils;
import com.onthegomap.planetiler.reader.SimpleFeature;
import com.onthegomap.planetiler.reader.WithTags;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class TagValueProducerTest {
  private static void testGet(TagValueProducer tvp, Map<String, Object> tags, String key, Object expected) {
    var wrapped = WithTags.from(tags);
    assertEquals(expected, tvp.mapTags(wrapped).get(key));
    assertEquals(expected, tvp.valueForKey(wrapped, key));
    assertEquals(expected, tvp.valueGetterForKey(key).apply(wrapped, key));
    assertEquals(expected, tvp.valueProducerForKey(key)
      .apply(new Contexts.ProcessFeature(SimpleFeature.create(GeoUtils.EMPTY_GEOMETRY, tags), tvp)
        .createPostMatchContext(List.of())));
  }

  @Test
  void testEmptyTagValueProducer() {
    var tvp = new TagValueProducer(Map.of());
    testGet(tvp, Map.of(), "key", null);
    testGet(tvp, Map.of("key", 1), "key", 1);
    testGet(tvp, Map.of("key", 1), "other", null);
  }

  @Test
  void testNullTagValueProducer() {
    var tvp = new TagValueProducer(null);
    testGet(tvp, Map.of(), "key", null);
  }

  @Test
  void testParseTypes() {
    var tvp = new TagValueProducer(Map.of(
      "int", "integer",
      "double", Map.of("type", "double"),
      "direction", Map.of("type", "direction")
    ));
    testGet(tvp, Map.of(), "int", null);
    testGet(tvp, Map.of(), "double", null);
    testGet(tvp, Map.of(), "direction", 0);

    testGet(tvp, Map.of("int", 1), "int", 1);
    testGet(tvp, Map.of("int", "1"), "int", 1);

    testGet(tvp, Map.of("direction", "-1"), "direction", -1);
  }

  @Test
  void testRemapKeys() {
    var tvp = new TagValueProducer(Map.of(
      "int2", Map.of("type", "integer", "input", "int"),
      "int3", Map.of("type", "integer", "input", "int2")
    ));
    testGet(tvp, Map.of("int", "1"), "int", "1");
    testGet(tvp, Map.of("int", "1"), "int2", 1);
    testGet(tvp, Map.of("int", "1"), "int3", 1);

    testGet(tvp, Map.of(), "int", null);
    testGet(tvp, Map.of(), "int2", null);
    testGet(tvp, Map.of(), "int3", null);
  }
}
