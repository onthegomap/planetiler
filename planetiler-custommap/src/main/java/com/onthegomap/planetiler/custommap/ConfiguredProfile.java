package com.onthegomap.planetiler.custommap;

import static com.onthegomap.planetiler.expression.MultiExpression.Entry;

import com.onthegomap.planetiler.FeatureCollector;
import com.onthegomap.planetiler.Profile;
import com.onthegomap.planetiler.custommap.configschema.FeatureLayer;
import com.onthegomap.planetiler.custommap.configschema.SchemaConfig;
import com.onthegomap.planetiler.expression.MultiExpression;
import com.onthegomap.planetiler.expression.MultiExpression.Index;
import com.onthegomap.planetiler.reader.SourceFeature;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * A profile configured from a yml file.
 */
public class ConfiguredProfile implements Profile {

  private final SchemaConfig schemaConfig;

  private final Index<ConfiguredFeature> featureLayerMatcher;

  public ConfiguredProfile(SchemaConfig schemaConfig) {
    this.schemaConfig = schemaConfig;

    Collection<FeatureLayer> layers = schemaConfig.layers();
    if (layers == null) {
      throw new IllegalArgumentException("No layers defined");
    }

    TagValueProducer tagValueProducer = new TagValueProducer(schemaConfig.inputMappings());

    List<MultiExpression.Entry<ConfiguredFeature>> configuredFeatureEntries = new ArrayList<>();

    for (var layer : layers) {
      String layerName = layer.name();
      for (var feature : layer.features()) {
        var configuredFeature = new ConfiguredFeature(layerName, tagValueProducer, feature);
        configuredFeatureEntries.add(
          new Entry<>(configuredFeature, configuredFeature.matchExpression()));
      }
    }

    featureLayerMatcher = MultiExpression.of(configuredFeatureEntries).index();
  }

  @Override
  public String name() {
    return schemaConfig.schemaName();
  }

  @Override
  public String attribution() {
    return schemaConfig.attribution();
  }

  @Override
  public void processFeature(SourceFeature sourceFeature, FeatureCollector featureCollector) {
    featureLayerMatcher.getMatches(sourceFeature)
      .forEach(configuredFeature -> configuredFeature.processFeature(sourceFeature, featureCollector));
  }

  @Override
  public String description() {
    return schemaConfig.schemaDescription();
  }
}
