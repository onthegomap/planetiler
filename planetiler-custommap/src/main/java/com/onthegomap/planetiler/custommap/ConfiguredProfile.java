package com.onthegomap.planetiler.custommap;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.onthegomap.planetiler.FeatureCollector;
import com.onthegomap.planetiler.Profile;
import com.onthegomap.planetiler.reader.SourceFeature;

public class ConfiguredProfile implements Profile {

  private String schemaName;
  private String attribution;
  private String description;

  private List<CustomFeature> features = new ArrayList<>();

  public ConfiguredProfile(Map<String, Object> profileDef) {
    schemaName = YamlParser.getString(profileDef, "shemaName");
    attribution = YamlParser.getString(profileDef, "attribution",
      "<a href=\\\"https://www.openstreetmap.org/copyright\\\" target=\\\"_blank\\\">&copy; OpenStreetMap contributors</a>");
    description = YamlParser.getString(profileDef, "schemaDescription");

    List<Map<String, Object>> layers = (List<Map<String, Object>>) profileDef.get("layers");
    if (layers == null) {
      return;
    }

    layers.stream().forEach(layer -> {
      String layerName = YamlParser.getString(layer, "name");
      List<Object> featureDefs = (List<Object>) layer.get("features");

      for (int j = 0; j < featureDefs.size(); j++) {
        features.add(new ConfiguredFeature(layerName, (Map<String, Object>) featureDefs.get(j)));
      }
    });
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
