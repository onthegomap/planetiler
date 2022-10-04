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
import java.nio.file.Path;
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

  private final SchemaConfig schema;

  private final Map<String, Index<ConfiguredFeature>> featureLayerMatcher;
  private final TagValueProducer tagValueProducer;
  private final Contexts.Root rootContext;

  public ConfiguredProfile(SchemaConfig schema, Contexts.Root rootContext) {
    this.schema = schema;
    this.rootContext = rootContext;

    Collection<FeatureLayer> layers = schema.layers();
    if (layers == null || layers.isEmpty()) {
      throw new IllegalArgumentException("No layers defined");
    }

    tagValueProducer = new TagValueProducer(schema.inputMappings());

    Map<String, List<MultiExpression.Entry<ConfiguredFeature>>> configuredFeatureEntries = new HashMap<>();

    for (var layer : layers) {
      String layerId = layer.id();
      for (var feature : layer.features()) {
        var configuredFeature = new ConfiguredFeature(layerId, tagValueProducer, feature, rootContext);
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
    return schema.schemaName();
  }

  @Override
  public String attribution() {
    return schema.attribution();
  }

  @Override
  public void processFeature(SourceFeature sourceFeature, FeatureCollector featureCollector) {
    var context = rootContext.createProcessFeatureContext(sourceFeature, tagValueProducer);
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
    return schema.schemaDescription();
  }

  public List<Source> sources() {
    List<Source> sources = new ArrayList<>();
    schema.sources().forEach((key, value) -> {
      var url = ConfigExpressionParser.tryStaticEvaluate(rootContext, value.url(), String.class).get();
      var path = ConfigExpressionParser.tryStaticEvaluate(rootContext, value.localPath(), String.class).get();
      sources.add(new Source(key, value.type(), url, path == null ? null : Path.of(path)));
    });
    return sources;
  }
}
