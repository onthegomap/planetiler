package com.onthegomap.planetiler.custommap;

import static com.onthegomap.planetiler.custommap.TagCriteria.matcher;
import static com.onthegomap.planetiler.expression.Expression.not;

import com.onthegomap.planetiler.FeatureCollector;
import com.onthegomap.planetiler.FeatureCollector.Feature;
import com.onthegomap.planetiler.custommap.configschema.AttributeDefinition;
import com.onthegomap.planetiler.custommap.configschema.FeatureItem;
import com.onthegomap.planetiler.custommap.configschema.ZoomOverride;
import com.onthegomap.planetiler.expression.Expression;
import com.onthegomap.planetiler.expression.MultiExpression;
import com.onthegomap.planetiler.expression.MultiExpression.Entry;
import com.onthegomap.planetiler.expression.MultiExpression.Index;
import com.onthegomap.planetiler.geo.GeometryException;
import com.onthegomap.planetiler.geo.GeometryType;
import com.onthegomap.planetiler.reader.SourceFeature;
import com.onthegomap.planetiler.reader.WithTags;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.ToIntFunction;

/**
 * A map feature, configured from a YML configuration file.
 *
 * {@link #matchExpression()} returns a filtering expression to limit input elements to ones this feature cares about,
 * and {@link #processFeature(SourceFeature, FeatureCollector)} processes matching elements.
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

  private final List<BiConsumer<SourceFeature, Feature>> attributeProcessors;

  public ConfiguredFeature(String layerName, TagValueProducer tagValueProducer, FeatureItem feature) {
    sources = new HashSet<>(feature.sources());

    GeometryType geometryType = feature.geometry();

    //Test to determine whether this type of geometry is included
    geometryTest = geometryType.featureTest();

    //Factory to treat OSM tag values as specific data type values
    this.tagValueProducer = tagValueProducer;

    //Test to determine whether this feature is included based on tagging
    if (feature.includeWhen() == null) {
      tagTest = Expression.TRUE;
    } else {
      tagTest = matcher(feature.includeWhen(), tagValueProducer);
    }

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
  private Function<WithTags, Object> attributeValueProducer(AttributeDefinition attribute) {

    Object constVal = attribute.constantValue();
    if (constVal != null) {
      return sf -> constVal;
    }

    String tagVal = attribute.tagValue();
    if (tagVal != null) {
      return tagValueProducer.valueProducerForKey(tagVal);
    }

    //Default to producing a tag identical to the input
    return tagValueProducer.valueProducerForKey(attribute.key());
  }

  /**
   * Generate logic which determines the minimum zoom level for a feature based on a configured pixel size limit.
   *
   * @param minTilePercent - minimum percentage of a tile that a feature must cover to be shown
   * @param minZoom        - global minimum zoom for this feature
   * @param minZoomByValue - map of tag values to zoom level
   * @return minimum zoom function
   */
  private static BiFunction<SourceFeature, Object, Integer> attributeZoomThreshold(Double minTilePercent, int minZoom,
    Map<Object, Integer> minZoomByValue) {

    if (minZoom == 0 && minZoomByValue.isEmpty()) {
      return null;
    }

    ToIntFunction<SourceFeature> staticZooms = sf -> Math.max(minZoom, minZoomFromTilePercent(sf, minTilePercent));

    if (minZoomByValue.isEmpty()) {
      return (sf, key) -> staticZooms.applyAsInt(sf);
    }

    //Attribute value-specific zooms override static zooms
    return (sourceFeature, key) -> minZoomByValue.getOrDefault(key, staticZooms.applyAsInt(sourceFeature));
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
  private BiConsumer<SourceFeature, Feature> attributeProcessor(AttributeDefinition attribute) {
    var tagKey = attribute.key();

    var attributeMinZoom = attribute.minZoom();
    attributeMinZoom = attributeMinZoom == null ? 0 : attributeMinZoom;

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

    BiFunction<SourceFeature, Object, Integer> attributeZoomProducer =
      attributeZoomThreshold(minTileCoverage, attributeMinZoom, minZoomByValue);

    if (attributeZoomProducer != null) {
      return (sf, f) -> {
        if (attributeTest.evaluate(sf)) {
          Object value = attributeValueProducer.apply(sf);
          f.setAttrWithMinzoom(tagKey, value, attributeZoomProducer.apply(sf, value));
        }
      };
    }

    return (sf, f) -> {
      if (attributeTest.evaluate(sf)) {
        f.setAttr(tagKey, attributeValueProducer.apply(sf));
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
   * @param sourceFeature - input source feature
   * @param features      - output rendered feature collector
   */
  public void processFeature(SourceFeature sourceFeature, FeatureCollector features) {

    //Ensure that this feature is from the correct source
    if (!sources.contains(sourceFeature.getSource())) {
      return;
    }

    var minZoom = zoomOverride.getOrElse(sourceFeature, featureMinZoom);

    var f = geometryFactory.apply(features)
      .setMinZoom(minZoom)
      .setMaxZoom(featureMaxZoom);

    for (var processor : attributeProcessors) {
      processor.accept(sourceFeature, f);
    }
  }
}
