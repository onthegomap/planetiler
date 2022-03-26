package com.onthegomap.planetiler.custommap;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import com.fasterxml.jackson.databind.JsonNode;
import com.onthegomap.planetiler.FeatureCollector;
import com.onthegomap.planetiler.FeatureCollector.Feature;
import com.onthegomap.planetiler.reader.SourceFeature;
import com.onthegomap.planetiler.util.ZoomFunction;

public class ConfiguredFeature implements CustomFeature {

  private Set<String> sources = new HashSet<>();
  private Set<Predicate<SourceFeature>> geometryTests;
  private Predicate<SourceFeature> tagTest;
  private String layerName;
  private JsonNode attributes;

  private static final double BUFFER_SIZE = 4.0;

  private static List<BiConsumer<SourceFeature, Feature>> attributeProcessors = new ArrayList<>();

  public ConfiguredFeature(String layerName, JsonNode featureDef) {
    this.layerName = layerName;
    sources = JsonParser.extractStringSet(featureDef.get("sources"));
    JsonNode includeWhen = featureDef.get("includeWhen");

    Set<String> geometry = JsonParser.extractStringSet(includeWhen.get("geometry"));

    geometryTests = geometry.stream()
      .map(ConfiguredFeature::geometryTest)
      .collect(Collectors.toSet());

    tagTest = tagTest(includeWhen.get("tag"));

    attributes = featureDef.get("attributes");
    for (int i = 0; i < attributes.size(); i++) {
      attributeProcessors.add(attributeProcessor(attributes.get(i)));
    }
  }

  private static Function<SourceFeature, Object> attributeValueProducer(JsonNode json) {

    String constVal = JsonParser.getStringField(json, "constantValue");
    if (constVal != null) {
      return sf -> constVal;
    }

    String tagVal = JsonParser.getStringField(json, "tagValue");
    if (tagVal != null) {
      Function<Object, Object> typeConverter = typeConverter(JsonParser.getStringField(json, "dataType"));
      return sf -> typeConverter.apply(sf.getTag(tagVal, null));
    }
    throw new IllegalArgumentException("No value producer specified");
  }

  private static Function<Object, Object> typeConverter(String type) {

    switch (type) {
      case "boolean":
        return booleanTypeConverter();
    }

    //Default: pass through
    return in -> {
      return in;
    };
  }

  private static Set<String> booleanTrue = new HashSet<>(Arrays.<String>asList("true", "yes", "1"));
  private static Set<String> booleanFalse = new HashSet<>(Arrays.<String>asList("false", "no", "0"));

  private static Function<Object, Object> booleanTypeConverter() {
    return s -> {
      if (booleanTrue.contains(s)) {
        return Boolean.TRUE;
      }
      if (booleanFalse.contains(s)) {
        return Boolean.FALSE;
      }
      return null;
    };
  }

  private static BiConsumer<SourceFeature, Feature> attributeProcessor(JsonNode json) {
    System.err.println(json);

    String tagKey = json.get("key").asText();
    Integer configuredMinZoom = JsonParser.getIntField(json, "minZoom");

    int minZoom = configuredMinZoom == null ? 0 : configuredMinZoom;
    Function<SourceFeature, Object> attributeValueProducer = attributeValueProducer(json);

    Predicate<SourceFeature> attributeTest = tagTest(json.get("includeWhen"));

    return (sf, f) -> {
      if (attributeTest.test(sf)) {
        f.setAttrWithMinzoom(tagKey, attributeValueProducer.apply(sf), minZoom);
      }
    };
  }

  private static Predicate<SourceFeature> geometryTest(String type) {
    switch (type) {
      case "polygon":
        return sf -> sf.canBePolygon();
      default:
        throw new IllegalArgumentException("Unhandled geometry type " + type);
    }
  }

  private static Predicate<SourceFeature> tagTest(JsonNode tagConstraint) {
    if (tagConstraint == null) {
      return sf -> true;
    }
    String keyTest = JsonParser.getStringField(tagConstraint, "key");
    Set<String> valTest = JsonParser.extractStringSet(tagConstraint.get("value"));
    if (keyTest == null) {
      return sf -> true;
    }
    return tagTest(keyTest, valTest);
  }

  private static Predicate<SourceFeature> tagTest(String key, Collection<String> values) {
    return sf -> {
      if (sf.hasTag(key)) {
        return values.contains(sf.getTag(key).toString());
      }
      return false;
    };
  }

  @Override
  public boolean includeWhen(SourceFeature sourceFeature) {
    //Is this from the right source?
    if (!sources.contains(sourceFeature.getSource())) {
      return false;
    }

    //Is this the right type of geometry?
    if (geometryTests.stream()
      .filter(p -> p.test(sourceFeature))
      .findAny()
      .isEmpty()) {
      return false;
    }

    //Check for matching tags
    return tagTest.test(sourceFeature);
  }

  @Override
  public void processFeature(SourceFeature sourceFeature, FeatureCollector features) {

    Feature f = features.polygon(layerName)
      .setBufferPixels(BUFFER_SIZE)
      .setAttrWithMinzoom("natural", "water", 0)
      .setZoomRange(0, 14) //TODO
      .setMinPixelSize(2)
      // and also whenever you set a label grid size limit, make sure you increase the buffer size so no
      // label grid squares will be the consistent between adjacent tiles
      .setBufferPixelOverrides(ZoomFunction.maxZoom(12, 32));

    attributeProcessors.forEach(p -> p.accept(sourceFeature, f));

  }
}
