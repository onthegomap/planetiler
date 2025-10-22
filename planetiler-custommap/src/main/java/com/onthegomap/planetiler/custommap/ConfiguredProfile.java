package com.onthegomap.planetiler.custommap;

import static com.onthegomap.planetiler.expression.MultiExpression.Entry;

import com.onthegomap.planetiler.FeatureCollector;
import com.onthegomap.planetiler.FeatureMerge;
import com.onthegomap.planetiler.Profile;
import com.onthegomap.planetiler.VectorTile;
import com.onthegomap.planetiler.custommap.configschema.FeatureLayer;
import com.onthegomap.planetiler.custommap.configschema.SchemaConfig;
import com.onthegomap.planetiler.expression.MultiExpression;
import com.onthegomap.planetiler.expression.MultiExpression.Index;
import com.onthegomap.planetiler.geo.GeometryException;
import com.onthegomap.planetiler.reader.SourceFeature;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * A profile configured from a yml file.
 */
public class ConfiguredProfile implements Profile {

  private final SchemaConfig schema;
  private final Map<String, FeatureLayer> layersById = new HashMap<>();
  private final Index<ConfiguredFeature> featureLayerMatcher;
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

    List<MultiExpression.Entry<ConfiguredFeature>> configuredFeatureEntries = new ArrayList<>();

    for (var layer : layers) {
      String layerId = layer.id();
      layersById.put(layerId, layer);
      for (var feature : layer.features()) {
        var configuredFeature = new ConfiguredFeature(layer, tagValueProducer, feature, rootContext);
        var entry = new Entry<>(configuredFeature, configuredFeature.matchExpression());
        configuredFeatureEntries.add(entry);
      }
    }

    featureLayerMatcher = MultiExpression.of(configuredFeatureEntries).index();

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
  public boolean isOverlay() {
    return schema.isOverlay();
  }

  @Override
  public void processFeature(SourceFeature sourceFeature, FeatureCollector featureCollector) {
    var context = rootContext.createProcessFeatureContext(sourceFeature, tagValueProducer);
    var matches = featureLayerMatcher.getMatchesWithTriggers(context);
    for (var configuredFeature : matches) {
      configuredFeature.match().processFeature(
        context.createPostMatchContext(configuredFeature.keys()),
        featureCollector
      );
    }
  }

  @Override
  public List<VectorTile.Feature> postProcessLayerFeatures(String layer, int zoom,
    List<VectorTile.Feature> items) throws GeometryException {
    FeatureLayer featureLayer = findFeatureLayer(layer);

    if (featureLayer.postProcess() == null) {
      return items;
    }

    var config = rootContext.config();
    if (featureLayer.postProcess().mergeLineStrings() != null) {
      var merge = featureLayer.postProcess().mergeLineStrings();
      var minLength = Objects.requireNonNullElse(
        (zoom == config.maxzoomForRendering()) ?
          merge.minLengthAtMaxZoom() :
          merge.minLength(),
        config.minFeatureSize(zoom));
      var tolerance = Objects.requireNonNullElse(
        (zoom == config.maxzoomForRendering()) ?
          merge.toleranceAtMaxZoom() :
          merge.tolerance(),
        config.tolerance(zoom));
      var buffer = Objects.requireNonNullElse(merge.buffer(), 4.0);

      items = FeatureMerge.mergeLineStrings(items,
        minLength, // after merging, remove lines that are still less than {minLength}px long
        tolerance, // simplify output linestrings using a {tolerance}px tolerance
        buffer // remove any detail more than {buffer}px outside the tile boundary
      );
    }

    var merge = featureLayer.postProcess().mergePolygons();
    if (merge != null) {
      var minArea = Objects.requireNonNullElse(
        (zoom == config.maxzoomForRendering()) ?
          merge.minAreaAtMaxZoom() :
          merge.minArea(),
        config.minFeatureSize(zoom)*config.minFeatureSize(zoom));

      items = FeatureMerge.mergeOverlappingPolygons(items,
        minArea // after merging, remove polygons that are still less than {minArea} in square tile pixels
      );
    }

    return items;
  }

  @Override
  public String description() {
    return schema.schemaDescription();
  }

  public List<Source> sources() {
    List<Source> sources = new ArrayList<>();
    schema.sources().forEach((key, value) -> {
      var url = evaluate(value.url(), String.class);
      var path = evaluate(value.localPath(), String.class);
      var projection = evaluate(value.projection(), String.class);
      sources.add(new Source(key, value.type(), url, path == null ? null : Path.of(path), projection));
    });
    return sources;
  }

  private <T> T evaluate(Object expression, Class<T> returnType) {
    return ConfigExpressionParser.tryStaticEvaluate(rootContext, expression, returnType).get();
  }

  public FeatureLayer findFeatureLayer(String layerId) {
    return layersById.get(layerId);
  }
}
