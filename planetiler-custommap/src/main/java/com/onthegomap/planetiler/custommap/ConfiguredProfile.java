package com.onthegomap.planetiler.custommap;

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.databind.JsonNode;
import com.onthegomap.planetiler.FeatureCollector;
import com.onthegomap.planetiler.Profile;
import com.onthegomap.planetiler.custommap.features.Waterway;
import com.onthegomap.planetiler.reader.SourceFeature;

public class ConfiguredProfile implements Profile {

  private String schemaName;
  private String attribution;
  private String description;

  private List<CustomFeature> features = new ArrayList<>();

  public ConfiguredProfile(JsonNode profileDef) {
    schemaName = profileDef.get("schemaName").asText();
    attribution = profileDef.get("attribution").asText(
      "<a href=\\\"https://www.openstreetmap.org/copyright\\\" target=\\\"_blank\\\">&copy; OpenStreetMap contributors</a>");
    description = profileDef.get("schemaDescription").asText("");

    JsonNode layers = profileDef.get("layers");

    for (int i = 0; i < layers.size(); i++) {
      JsonNode layer = layers.get(i);
      String layerName = layer.get("name").asText();

      JsonNode featureDefs = layer.get("features");

      for (int j = 0; j < featureDefs.size(); j++) {
        features.add(new ConfiguredFeature(layerName, featureDefs.get(j)));
      }
    }

    features.add(new Waterway());
  }

  @Override
  public String name() {
    return schemaName;
  }


  @Override
  public String attribution() {
    return attribution;
  }

  @Override
  public void processFeature(SourceFeature sourceFeature, FeatureCollector featureCollector) {
    features
      .stream()
      .filter(cf -> cf.includeWhen(sourceFeature))
      .forEach(cf -> cf.processFeature(sourceFeature, featureCollector));
  }

  @Override
  public String description() {
    return description;
  }
}
