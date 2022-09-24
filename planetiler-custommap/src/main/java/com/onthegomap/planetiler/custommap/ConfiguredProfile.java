package com.onthegomap.planetiler.custommap;

import static com.onthegomap.planetiler.expression.MultiExpression.Entry;
import static java.util.Map.entry;

import com.onthegomap.planetiler.FeatureCollector;
import com.onthegomap.planetiler.Profile;
import com.onthegomap.planetiler.custommap.configschema.FeatureLayer;
import com.onthegomap.planetiler.custommap.configschema.SchemaConfig;
import com.onthegomap.planetiler.expression.MultiExpression;
import com.onthegomap.planetiler.expression.MultiExpression.Index;
import com.onthegomap.planetiler.reader.SourceFeature;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * A profile configured from a yml file.
 */
public class ConfiguredProfile implements Profile {

  private final SchemaConfig schemaConfig;

  private final Map<String, Index<ConfiguredFeature>> featureLayerMatcher;
  private final TagValueProducer tagValueProducer;

  public ConfiguredProfile(SchemaConfig schemaConfig) {
    this.schemaConfig = schemaConfig;

    Collection<FeatureLayer> layers = schemaConfig.layers();
    if (layers == null || layers.isEmpty()) {
      throw new IllegalArgumentException("No layers defined");
    }

    tagValueProducer = new TagValueProducer(schemaConfig.inputMappings());

    Map<String, List<MultiExpression.Entry<ConfiguredFeature>>> configuredFeatureEntries = new HashMap<>();

    for (var layer : layers) {
      String layerId = layer.id();
      for (var feature : layer.features()) {
        var configuredFeature = new ConfiguredFeature(layerId, tagValueProducer, feature);
        var entry = new Entry<>(configuredFeature, configuredFeature.matchExpression());
        for (var source : feature.source()) {
          var list = configuredFeatureEntries.computeIfAbsent(source, s -> new ArrayList<>());
          list.add(entry);
        }
      }
    }

    featureLayerMatcher = configuredFeatureEntries.entrySet().stream()
      .map(entry -> entry(entry.getKey(), MultiExpression.of(entry.getValue()).index()))
      .collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, Map.Entry::getValue));
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
    var context = new Contexts.ProcessFeature(sourceFeature, tagValueProducer);
    var index = featureLayerMatcher.get(sourceFeature.getSource());
    if (index != null) {
      var matches = index.getMatchesWithTriggers(context);
      for (var configuredFeature : matches) {
        configuredFeature.match().processFeature(
          context.createPostMatchContext(configuredFeature.keys()),
          featureCollector
        );
      }
    }
  }

  @Override
  public String description() {
    return schemaConfig.schemaDescription();
  }
}
