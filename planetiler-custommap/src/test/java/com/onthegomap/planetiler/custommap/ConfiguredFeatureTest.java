package com.onthegomap.planetiler.custommap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Predicate;

import org.junit.jupiter.api.Test;

import com.onthegomap.planetiler.custommap.configschema.AttributeDataType;
import com.onthegomap.planetiler.custommap.configschema.AttributeDefinition;
import com.onthegomap.planetiler.reader.SourceFeature;

public class ConfiguredFeatureTest {

  @Test
  public void testTagValueAttributeTest() {

    Map<String, Object> tags = new HashMap<>();
    tags.put("natural", "water");
    tags.put("name", "Little Pond");
    SourceFeature sf = new TestAreaSourceFeature(tags, "osm", "water");

    AttributeDefinition attributeDef = new AttributeDefinition();
    attributeDef.setKey("name");
    attributeDef.setTagValue("name");
    attributeDef.setDataType(AttributeDataType.string);

    Function<SourceFeature, Object> attributeValueProducer = ConfiguredFeature.attributeValueProducer(attributeDef);

    assertEquals("Little Pond", attributeValueProducer.apply(sf));

    Predicate<SourceFeature> attributeTest = ConfiguredFeature.attributeTagTest(null, attributeValueProducer);

    assertTrue(attributeTest.test(sf));
  }

  @Test
  public void testUnconstrainedTagAttributeTest() {
    Map<String, Object> tags = new HashMap<>();
    tags.put("natural", "water");
    tags.put("name", "Little Pond");
    SourceFeature sf = new TestAreaSourceFeature(tags, "osm", "water");

    AttributeDefinition attrDef = new AttributeDefinition();
    attrDef.setKey("name");
    attrDef.setTagValue("name");

    Function<SourceFeature, Object> attributeValueProducer = ConfiguredFeature.attributeValueProducer(attrDef);

    assertEquals("Little Pond", attributeValueProducer.apply(sf));

    Predicate<SourceFeature> attributeTest = ConfiguredFeature.attributeTagTest(null, attributeValueProducer);

    assertTrue(attributeTest.test(sf));
  }

}
