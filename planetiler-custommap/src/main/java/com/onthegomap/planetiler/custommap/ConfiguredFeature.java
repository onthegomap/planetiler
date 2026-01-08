package com.onthegomap.planetiler.custommap;

import static com.onthegomap.planetiler.custommap.expression.ConfigExpression.constOf;
import static com.onthegomap.planetiler.expression.Expression.not;

import com.onthegomap.planetiler.FeatureCollector;
import com.onthegomap.planetiler.FeatureCollector.Feature;
import com.onthegomap.planetiler.custommap.configschema.AttributeDefinition;
import com.onthegomap.planetiler.custommap.configschema.FeatureGeometry;
import com.onthegomap.planetiler.custommap.configschema.FeatureItem;
import com.onthegomap.planetiler.custommap.configschema.FeatureLayer;
import com.onthegomap.planetiler.custommap.expression.ScriptEnvironment;
import com.onthegomap.planetiler.expression.Expression;
import com.onthegomap.planetiler.geo.GeometryException;
import com.onthegomap.planetiler.reader.SourceFeature;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.ObjDoubleConsumer;

/**
 * A map feature, configured from a YML configuration file.
 *
 * {@link #matchExpression()} returns a filtering expression to limit input elements to ones this feature cares about,
 * and {@link #processFeature(Contexts.FeaturePostMatch, FeatureCollector)} processes matching elements.
 */
public class ConfiguredFeature {
  private static final double LOG4 = Math.log(4);
  private final Expression geometryTest;
  private final Function<FeatureCollector, Feature> geometryFactory;
  private final Expression tagTest;
  private final TagValueProducer tagValueProducer;
  private final List<BiConsumer<Contexts.FeaturePostMatch, Feature>> featureProcessors;
  private final Set<String> sources;
  private final ScriptEnvironment<Contexts.ProcessFeature> processFeatureContext;
  private final ScriptEnvironment<Contexts.FeatureAttribute> featureAttributeContext;
  private ScriptEnvironment<Contexts.FeaturePostMatch> featurePostMatchContext;
  private final boolean splitAtIntersections;


  public ConfiguredFeature(FeatureLayer layer, TagValueProducer tagValueProducer, FeatureItem feature,
    Contexts.Root rootContext) {
    sources = Set.copyOf(feature.source());

    FeatureGeometry geometryType = feature.geometry();

    //Test to determine whether this type of geometry is included
    geometryTest = geometryType.featureTest();

    //Factory to treat OSM tag values as specific data type values
    this.tagValueProducer = tagValueProducer;
    processFeatureContext = Contexts.ProcessFeature.description(rootContext);
    featurePostMatchContext = Contexts.FeaturePostMatch.description(rootContext);
    featureAttributeContext = Contexts.FeatureAttribute.description(rootContext);

    //Test to determine whether this feature is included based on tagging
    Expression filter;
    if (feature.includeWhen() == null) {
      filter = Expression.TRUE;
    } else {
      filter =
        BooleanExpressionParser.parse(feature.includeWhen(), tagValueProducer,
          processFeatureContext);
    }
    if (!feature.source().isEmpty()) {
      filter = Expression.and(
        filter,
        Expression.or(feature.source().stream().map(Expression::matchSource).toList())
      );
    }
    if (feature.excludeWhen() != null) {
      filter = Expression.and(
        filter,
        Expression.not(
          BooleanExpressionParser.parse(feature.excludeWhen(), tagValueProducer,
            processFeatureContext))
      );
    }
    tagTest = filter;

    //Factory to generate the right feature type from FeatureCollector
    geometryFactory = geometryType.newGeometryFactory(layer.id());

    //Configure logic for each attribute in the output tile
    List<BiConsumer<Contexts.FeaturePostMatch, Feature>> processors = new ArrayList<>();
    for (var attribute : feature.attributes()) {
      processors.add(attributeProcessor(attribute));
    }
    processors.add(makeFeatureProcessor(feature.minZoom(), Integer.class, Feature::setMinZoom));
    processors.add(makeFeatureProcessor(feature.maxZoom(), Integer.class, Feature::setMaxZoom));

    addPostProcessingImplications(layer, feature, processors, rootContext);

    // per-feature tolerance settings should take precedence over defaults from post-processing config
    processors.add(makeFeatureProcessor(feature.tolerance(), Double.class, Feature::setPixelTolerance));
    processors
      .add(makeFeatureProcessor(feature.toleranceAtMaxZoom(), Double.class, Feature::setPixelToleranceAtMaxZoom));

    featureProcessors = processors.stream().filter(Objects::nonNull).toList();
    splitAtIntersections = Boolean.TRUE.equals(feature.splitAtIntersections());
  }

  /** Consider implications of Post Processing on the feature's processors **/
  private void addPostProcessingImplications(FeatureLayer layer, FeatureItem feature,
    List<BiConsumer<Contexts.FeaturePostMatch, Feature>> processors,
    Contexts.Root rootContext) {
    var postProcess = layer.postProcess();

    // Consider min_size and min_size_at_max_zoom
    if (postProcess == null) {
      processors.add(makeFeatureProcessor(feature.minSize(), Double.class, Feature::setMinPixelSize));
      processors.add(makeFeatureProcessor(feature.minSizeAtMaxZoom(), Double.class, Feature::setMinPixelSizeAtMaxZoom));
      return;
    }
    // In order for Post-processing to receive all features, the default MinPixelSize* are zero when features are collected
    processors.add(
      makeFeatureProcessor(Objects.requireNonNullElse(feature.minSize(), 0), Double.class, Feature::setMinPixelSize));
    processors.add(makeFeatureProcessor(Objects.requireNonNullElse(feature.minSizeAtMaxZoom(), 0), Double.class,
      Feature::setMinPixelSizeAtMaxZoom));
    // Implications of tile_post_process.merge_line_strings
    var mergeLineStrings = postProcess.mergeLineStrings();
    if (mergeLineStrings != null) {
      processors.add(makeLineFeatureProcessor(mergeLineStrings.tolerance(), Feature::setPixelTolerance));
      processors
        .add(makeLineFeatureProcessor(mergeLineStrings.toleranceAtMaxZoom(), Feature::setPixelToleranceAtMaxZoom));
      // postProcess.mergeLineStrings.minLength* and postProcess.mergeLineStrings.buffer
      var bufferPixels = maxIgnoringNulls(mergeLineStrings.minLength(), mergeLineStrings.buffer());
      var bufferPixelsAtMaxZoom = maxIgnoringNulls(mergeLineStrings.minLengthAtMaxZoom(), mergeLineStrings.buffer());
      int maxZoom = rootContext.config().maxzoomForRendering();
      if (bufferPixels != null || bufferPixelsAtMaxZoom != null) {
        processors.add((context, f) -> {
          if (f.isLine()) {
            f.setBufferPixelOverrides(z -> z == maxZoom ? bufferPixelsAtMaxZoom : bufferPixels);
          }
        });
      }

    }
    // Implications of tile_post_process.merge_polygons
    var mergePolygons = postProcess.mergePolygons();
    if (mergePolygons != null) {
      // postProcess.mergePolygons.tolerance*
      processors.add(makePolygonFeatureProcessor(mergePolygons.tolerance(), Feature::setPixelTolerance));
      processors
        .add(makePolygonFeatureProcessor(mergePolygons.toleranceAtMaxZoom(), Feature::setPixelToleranceAtMaxZoom));
      // TODO: postProcess.mergeLineStrings.minArea*
    }
  }

  private <T> BiConsumer<Contexts.FeaturePostMatch, Feature> makeFeatureProcessor(Object input, Class<T> clazz,
    BiConsumer<Feature, T> consumer) {
    if (input == null) {
      return null;
    }
    var expression = ConfigExpressionParser.parse(
      input,
      tagValueProducer,
      featurePostMatchContext,
      clazz
    );
    if (expression.equals(constOf(null))) {
      return null;
    }
    return (context, feature) -> {
      var result = expression.apply(context);
      if (result != null) {
        consumer.accept(feature, result);
      }
    };
  }

  private BiConsumer<Contexts.FeaturePostMatch, Feature> makeLineFeatureProcessor(Double input,
    ObjDoubleConsumer<Feature> consumer) {
    if (input == null) {
      return null;
    }
    return (context, feature) -> {
      if (feature.isLine()) {
        consumer.accept(feature, input);
      }
    };
  }

  private BiConsumer<Contexts.FeaturePostMatch, Feature> makePolygonFeatureProcessor(Double input,
    ObjDoubleConsumer<Feature> consumer) {
    if (input == null) {
      return null;
    }
    return (context, feature) -> {
      if (feature.isPolygon()) {
        consumer.accept(feature, input);
      }
    };
  }

  private static int minZoomFromTilePercent(SourceFeature sf, Double minTilePercent) {
    if (minTilePercent == null) {
      return 0;
    }
    try {
      return (int) (Math.log(minTilePercent / sf.area()) / LOG4);
    } catch (GeometryException e) {
      return 14;
    }
  }

  /**
   * Produces logic that generates attribute values based on configuration and input data. If both a constantValue
   * configuration and a tagValue configuration are set, this is likely a mistake, and the constantValue will take
   * precedence.
   *
   * @param attribute - attribute definition configured from YML
   * @return a function that generates an attribute value from a {@link SourceFeature} based on an attribute
   *         configuration.
   */
  private Function<Contexts.FeaturePostMatch, Object> attributeValueProducer(AttributeDefinition attribute) {
    Object type = attribute.type();

    // some expression features are hoisted to the top-level for attribute values for brevity,
    // so just map them to what the equivalent expression syntax would be and parse as an expression.
    Map<String, Object> value = new HashMap<>();
    if ("match_key".equals(type)) {
      value.put("value", "${match_key}");
    } else if ("match_value".equals(type)) {
      value.put("value", "${match_value}");
    } else {
      if (type != null) {
        value.put("type", type);
      }
      if (attribute.coalesce() != null) {
        value.put("coalesce", attribute.coalesce());
      } else if (attribute.value() != null) {
        value.put("value", attribute.value());
      } else if (attribute.tagValue() != null) {
        value.put("tag_value", attribute.tagValue());
      } else if (attribute.argValue() != null) {
        value.put("arg_value", attribute.argValue());
      } else {
        value.put("tag_value", attribute.key());
      }
    }

    return ConfigExpressionParser.parse(value, tagValueProducer, featurePostMatchContext, Object.class);
  }

  /**
   * Generate logic which determines the minimum zoom level for a feature based on a configured pixel size limit.
   *
   * @param minTilePercent - minimum percentage of a tile that a feature must cover to be shown
   * @param rawMinZoom     - global minimum zoom for this feature, or an expression providing the min zoom dynamically
   * @param minZoomByValue - map of tag values to zoom level
   * @return minimum zoom function
   */
  private Function<Contexts.FeatureAttribute, Integer> attributeZoomThreshold(
    Double minTilePercent, Object rawMinZoom, Map<Object, Integer> minZoomByValue) {

    var result = ConfigExpressionParser.parse(rawMinZoom, tagValueProducer,
      featureAttributeContext, Integer.class);

    if ((result.equals(constOf(0)) ||
      result.equals(constOf(null))) && minZoomByValue.isEmpty()) {
      return null;
    }

    if (minZoomByValue.isEmpty()) {
      return context -> Math.max(result.apply(context), minZoomFromTilePercent(context.feature(), minTilePercent));
    }

    //Attribute value-specific zooms override static zooms
    return context -> {
      var value = minZoomByValue.get(context.value());
      return value != null ? value :
        Math.max(result.apply(context), minZoomFromTilePercent(context.feature(), minTilePercent));
    };
  }

  /**
   * Generates a function which produces a fully-configured attribute for a feature.
   *
   * @param attribute - configuration for this attribute
   * @return processing logic
   */
  private BiConsumer<Contexts.FeaturePostMatch, Feature> attributeProcessor(AttributeDefinition attribute) {
    var tagKey = attribute.key();

    Object attributeMinZoom = attribute.minZoom();
    attributeMinZoom = attributeMinZoom == null ? "0" : attributeMinZoom;

    var minZoomByValue = attribute.minZoomByValue();
    minZoomByValue = minZoomByValue == null ? Map.of() : minZoomByValue;

    //Workaround because numeric keys are mapped as String
    minZoomByValue = tagValueProducer.remapKeysByType(tagKey, minZoomByValue);

    var attributeValueProducer = attributeValueProducer(attribute);
    var fallback = attribute.fallback();

    var attrIncludeWhen = attribute.includeWhen();
    var attrExcludeWhen = attribute.excludeWhen();

    var attributeTest =
      Expression.and(
        attrIncludeWhen == null ? Expression.TRUE :
          BooleanExpressionParser.parse(attrIncludeWhen, tagValueProducer,
            featurePostMatchContext),
        attrExcludeWhen == null ? Expression.TRUE :
          not(BooleanExpressionParser.parse(attrExcludeWhen, tagValueProducer,
            featurePostMatchContext))
      ).simplify();

    var minTileCoverage = attrIncludeWhen == null ? null : attribute.minTileCoverSize();

    Function<Contexts.FeatureAttribute, Integer> attributeZoomProducer =
      attributeZoomThreshold(minTileCoverage, attributeMinZoom, minZoomByValue);

    return (context, f) -> {
      Object value = null;
      if (attributeTest.evaluate(context)) {
        value = attributeValueProducer.apply(context);
        if ("".equals(value)) {
          value = null;
        }
      }
      if (value == null) {
        value = fallback;
      }
      if (value != null) {
        if (attributeZoomProducer != null) {
          Integer minzoom = attributeZoomProducer.apply(context.createAttrZoomContext(value));
          if (minzoom != null) {
            f.setAttrWithMinzoom(tagKey, value, minzoom);
          } else {
            f.setAttr(tagKey, value);
          }
        } else {
          f.setAttr(tagKey, value);
        }
      }
    };
  }

  /**
   * Returns an expression that evaluates to true if a source feature should be included in the output.
   */
  public Expression matchExpression() {
    return Expression.and(geometryTest, tagTest);
  }

  /**
   * Generates a tile feature based on a source feature.
   *
   * @param context  The evaluation context containing the source feature
   * @param features output rendered feature collector
   */
  public void processFeature(Contexts.FeaturePostMatch context, FeatureCollector features) {
    var sourceFeature = context.feature();

    // Ensure that this feature is from the correct source (index should enforce this, so just check when assertions enabled)
    assert sources.isEmpty() || sources.contains(sourceFeature.getSource());

    var f = geometryFactory.apply(features);
    for (var processor : featureProcessors) {
      processor.accept(context, f);
    }
  }

  private Double maxIgnoringNulls(Double a, Double b) {
    if (a == null)
      return b;
    if (b == null)
      return a;
    return Double.max(a, b);
  }

  public boolean splitAtIntersections() {
    return splitAtIntersections;
  }
}
