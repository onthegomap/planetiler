package com.onthegomap.planetiler.custommap;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.BiConsumer;
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

    JsonNode tagConstraint = includeWhen.get("tag");
    if (tagConstraint == null) {
      tagTest = sf -> true;
    } else {
      String keyTest = tagConstraint.get("key").asText();
      Set<String> valTest = JsonParser.extractStringSet(tagConstraint.get("value"));
      tagTest = tagTest(keyTest, valTest);
    }

    attributes = featureDef.get("attributes");
    for (int i = 0; i < attributes.size(); i++) {
      attributeProcessors.add(attributeProcessor(attributes.get(i)));
    }
  }

  private static BiConsumer<SourceFeature, Feature> attributeProcessor(JsonNode json) {
    System.err.println(json);
    return (sf, f) -> {

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
