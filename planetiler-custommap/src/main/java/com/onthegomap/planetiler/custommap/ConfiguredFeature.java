package com.onthegomap.planetiler.custommap;

import static com.onthegomap.planetiler.custommap.TagCriteria.matcher;
import static com.onthegomap.planetiler.expression.Expression.not;

import com.onthegomap.planetiler.FeatureCollector;
import com.onthegomap.planetiler.FeatureCollector.Feature;
import com.onthegomap.planetiler.custommap.configschema.AttributeDefinition;
import com.onthegomap.planetiler.custommap.configschema.FeatureGeometry;
import com.onthegomap.planetiler.custommap.configschema.FeatureItem;
import com.onthegomap.planetiler.custommap.configschema.ZoomOverride;
import com.onthegomap.planetiler.custommap.expression.ConfigExpression;
import com.onthegomap.planetiler.custommap.expression.Contexts;
import com.onthegomap.planetiler.custommap.expression.ParseException;
import com.onthegomap.planetiler.expression.Expression;
import com.onthegomap.planetiler.expression.MultiExpression;
import com.onthegomap.planetiler.expression.MultiExpression.Entry;
import com.onthegomap.planetiler.expression.MultiExpression.Index;
import com.onthegomap.planetiler.geo.GeometryException;
import com.onthegomap.planetiler.reader.SourceFeature;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.ToIntFunction;

/**
 * A map feature, configured from a YML configuration file.
 *
 * {@link #matchExpression()} returns a filtering expression to limit input elements to ones this feature cares about,
 * and {@link #processFeature(Contexts.ProcessFeature.PostMatch, FeatureCollector)} processes matching elements.
 */
public class ConfiguredFeature {

  private final Set<String> sources;
  private final Expression geometryTest;
  private final Function<FeatureCollector, Feature> geometryFactory;
  private final Expression tagTest;
  private final Index<Integer> zoomOverride;
  private final Integer featureMinZoom;
  private final Integer featureMaxZoom;
  private final TagValueProducer tagValueProducer;

  private static final double LOG4 = Math.log(4);
  private static final Index<Integer> NO_ZOOM_OVERRIDE = MultiExpression.<Integer>of(List.of()).index();
  private static final Integer DEFAULT_MAX_ZOOM = 14;

  private final List<AttributeProcessor> attributeProcessors;


  @FunctionalInterface
  private interface AttributeProcessor {
    void process(Contexts.ProcessFeature.PostMatch context, Feature outputFeature);
  }

  public ConfiguredFeature(String layerName, TagValueProducer tagValueProducer, FeatureItem feature) {
    sources = new HashSet<>(feature.source());

    FeatureGeometry geometryType = feature.geometry();

    //Test to determine whether this type of geometry is included
    geometryTest = geometryType.featureTest();

    //Factory to treat OSM tag values as specific data type values
    this.tagValueProducer = tagValueProducer;

    //Test to determine whether this feature is included based on tagging
    Expression filter;
    if (feature.includeWhen() == null) {
      filter = Expression.TRUE;
    } else {
      filter = matcher(feature.includeWhen(), tagValueProducer);
    }
    if (feature.excludeWhen() != null) {
      filter = Expression.and(
        filter,
        Expression.not(matcher(feature.excludeWhen(), tagValueProducer))
      );
    }
    tagTest = filter;

    //Index of zoom ranges for a feature based on what tags are present.
    zoomOverride = zoomOverride(feature.zoom());

    //Test to determine at which zooms to include this feature based on tagging
    featureMinZoom = feature.minZoom() == null ? 0 : feature.minZoom();
    featureMaxZoom = feature.maxZoom() == null ? DEFAULT_MAX_ZOOM : feature.maxZoom();

    //Factory to generate the right feature type from FeatureCollector
    geometryFactory = geometryType.geometryFactory(layerName);

    //Configure logic for each attribute in the output tile
    attributeProcessors = feature.attributes()
      .stream()
      .map(this::attributeProcessor)
      .toList();
  }

  /**
   * Produce an index that matches tags from configuration and returns a minimum zoom level
   *
   * @param zoom the configured zoom overrides
   * @return an index
   */
  private Index<Integer> zoomOverride(Collection<ZoomOverride> zoom) {
    if (zoom == null || zoom.isEmpty()) {
      return NO_ZOOM_OVERRIDE;
    }

    return MultiExpression.of(
      zoom.stream()
        .map(this::generateOverrideExpression)
        .toList())
      .index();
  }

  /**
   * Takes the zoom override configuration for a single zoom level and returns an expression that matches tags for that
   * level.
   *
   * @param config zoom override for a single level
   * @return matching expression
   */
  private Entry<Integer> generateOverrideExpression(ZoomOverride config) {
    return MultiExpression.entry(config.min(),
      Expression.or(
        config.tag()
          .entrySet()
          .stream()
          .map(this::generateKeyExpression)
          .toList()));
  }

  /**
   * Returns an expression that matches against single key with one or more values
   *
   * @param keyExpression a map containing a key and one or more values
   * @return a matching expression
   */
  private Expression generateKeyExpression(Map.Entry<String, Object> keyExpression) {
    // Values are either a single value, or a collection
    String key = keyExpression.getKey();
    Object rawVal = keyExpression.getValue();

    if (rawVal instanceof List<?> tagValues) {
      return Expression.matchAnyTyped(key, tagValueProducer.valueGetterForKey(key), tagValues);
    }

    return Expression.matchAnyTyped(key, tagValueProducer.valueGetterForKey(key), rawVal);
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
  private TagValueProducer.Signature attributeValueProducer(AttributeDefinition attribute) {

    Object value = attribute.value();
    if (value != null) {
      String rawExpression = ConfigExpression.extractFromEscaped(value);
      if (rawExpression != null) {
        var expression = ConfigExpression.parse(rawExpression, Contexts.ProcessFeature.PostMatch.DESCRIPTION);
        return expression::evaluate;
      } else {
        return context -> value;
      }
    }

    Object constVal = attribute.constantValue();
    if (constVal != null) {
      return context -> constVal;
    }

    String tagVal = attribute.tagValue();
    if (tagVal != null) {
      return tagValueProducer.valueProducerForKey(tagVal);
    }

    String type = attribute.type();
    if ("match_key".equals(type)) {
      return Contexts.ProcessFeature.PostMatch::matchKey;
    } else if ("match_value".equals(type)) {
      return Contexts.ProcessFeature.PostMatch::matchValue;
    } else if (type != null) {
      throw new IllegalArgumentException("Unrecognized value for type: " + type);
    }

    //Default to producing a tag identical to the input
    return tagValueProducer.valueProducerForKey(attribute.key());
  }

  /**
   * Generate logic which determines the minimum zoom level for a feature based on a configured pixel size limit.
   *
   * @param minTilePercent - minimum percentage of a tile that a feature must cover to be shown
   * @param rawMinZoom     - global minimum zoom for this feature, or an expression providing the min zoom dynamically
   * @param minZoomByValue - map of tag values to zoom level
   * @return minimum zoom function
   */
  private static Function<Contexts.ProcessFeature.PostMatch.AttrZoom, Integer> attributeZoomThreshold(
    Double minTilePercent,
    Object rawMinZoom, Map<Object, Integer> minZoomByValue) {

    String expression = ConfigExpression.extractFromEscaped(rawMinZoom);
    if (expression != null) {
      return ConfigExpression.parse(expression, Contexts.ProcessFeature.PostMatch.AttrZoom.DESCRIPTION,
        Integer.class);
    }

    int minZoom;
    try {
      minZoom = Integer.parseInt(rawMinZoom.toString());
    } catch (NumberFormatException e) {
      throw new ParseException("Invalid min zoom: " + rawMinZoom);
    }

    if (minZoom == 0 && minZoomByValue.isEmpty()) {
      return null;
    }

    ToIntFunction<SourceFeature> staticZooms = sf -> Math.max(minZoom, minZoomFromTilePercent(sf, minTilePercent));

    if (minZoomByValue.isEmpty()) {
      return context -> staticZooms.applyAsInt(context.parent().parent().feature());
    }

    //Attribute value-specific zooms override static zooms
    return context -> minZoomByValue.getOrDefault(context.value(), staticZooms.applyAsInt(context.parent()
      .sourceFeature()));
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
   * Generates a function which produces a fully-configured attribute for a feature.
   *
   * @param attribute - configuration for this attribute
   * @return processing logic
   */
  private AttributeProcessor attributeProcessor(AttributeDefinition attribute) {
    var tagKey = attribute.key();

    Object attributeMinZoom = attribute.minZoom();
    attributeMinZoom = attributeMinZoom == null ? "0" : attributeMinZoom;

    var minZoomByValue = attribute.minZoomByValue();
    minZoomByValue = minZoomByValue == null ? Map.of() : minZoomByValue;

    //Workaround because numeric keys are mapped as String
    minZoomByValue = tagValueProducer.remapKeysByType(tagKey, minZoomByValue);

    var attributeValueProducer = attributeValueProducer(attribute);

    var attrIncludeWhen = attribute.includeWhen();
    var attrExcludeWhen = attribute.excludeWhen();

    var attributeTest =
      Expression.and(
        attrIncludeWhen == null ? Expression.TRUE : matcher(attrIncludeWhen, tagValueProducer),
        attrExcludeWhen == null ? Expression.TRUE : not(matcher(attrExcludeWhen, tagValueProducer))
      ).simplify();

    var minTileCoverage = attrIncludeWhen == null ? null : attribute.minTileCoverSize();

    Function<Contexts.ProcessFeature.PostMatch.AttrZoom, Integer> attributeZoomProducer =
      attributeZoomThreshold(minTileCoverage, attributeMinZoom, minZoomByValue);

    if (attributeZoomProducer != null) {
      return (context, f) -> {
        if (attributeTest.evaluate(context.parent().feature())) {
          Object value = attributeValueProducer.apply(context);
          f.setAttrWithMinzoom(tagKey, value, attributeZoomProducer.apply(context.createAttrZoomContext(value)));
        }
      };
    }

    return (context, f) -> {
      if (attributeTest.evaluate(context.parent().feature())) {
        f.setAttr(tagKey, attributeValueProducer.apply(context));
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
  public void processFeature(Contexts.ProcessFeature.PostMatch context, FeatureCollector features) {
    var sourceFeature = context.sourceFeature();

    //Ensure that this feature is from the correct source
    if (!sources.contains(sourceFeature.getSource())) {
      return;
    }

    var minZoom = zoomOverride.getOrElse(sourceFeature, featureMinZoom);

    var f = geometryFactory.apply(features)
      .setMinZoom(minZoom)
      .setMaxZoom(featureMaxZoom);

    for (var processor : attributeProcessors) {
      processor.process(context, f);
    }
  }
}
