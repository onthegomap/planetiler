package com.onthegomap.planetiler.custommap;

import static com.onthegomap.planetiler.custommap.configschema.TagCriteria.matcher;

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
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * A map feature, configured from a YML configuration file
 */
public class ConfiguredFeature {

  private final Collection<String> sources;
  private final Predicate<SourceFeature> geometryTest;
  private final Function<FeatureCollector, Feature> geometryFactory;
  private final Expression tagTest;
  private final Index<Integer> zoomOverride;
  private final Integer featureMinZoom;
  private final Integer featureMaxZoom;
  private final TagValueProducer tagValueProducer;

  private static final double LOG4 = Math.log(4);
  private static final Index<Integer> NO_ZOOM_OVERRIDE = MultiExpression.<Integer>of(List.of()).index();

  private List<BiConsumer<SourceFeature, Feature>> attributeProcessors = new ArrayList<>();

  public ConfiguredFeature(String layerName, TagValueProducer tagValueProducer, FeatureItem feature) {
    sources = feature.sources();

    GeometryType geometryType = feature.geometry();

    //Test to determine whether this type of geometry is included
    geometryTest = geometryTest(geometryType);

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
    featureMaxZoom = feature.maxZoom() == null ? 0 : feature.maxZoom();

    //Factory to generate the right feature type from FeatureCollector
    geometryFactory = geometryMapFeature(layerName, geometryType);

    //Configure logic for each attribute in the output tile
    feature.attributes()
      .stream()
      .map(this::attributeProcessor)
      .forEach(attributeProcessors::add);
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
      return Expression.matchAnyTyped(key, tagValueProducer.getValueGetter(key), tagValues);
    }

    return Expression.matchAnyTyped(key, tagValueProducer.getValueGetter(key), rawVal);
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
      return tagValueProducer.getValueProducer(tagVal);
    }

    //Default to producing a tag identical to the input
    return tagValueProducer.getValueProducer(attribute.key());
  }

  /**
   * Generate logic which determines the minimum zoom level for a feature based on a configured pixel size limit.
   * 
   * @param minTilePercent - minimum percentage of a tile that a feature must cover to be shown
   * @param minZoom        - global minimum zoom for this feature
   * @param minZoomByValue - map of tag values to zoom level
   * @return minimum zoom function
   */
  private static BiFunction<SourceFeature, Object, Byte> attributeZoomThreshold(Double minTilePercent, byte minZoom,
    Map<Object, Byte> minZoomByValue) {

    if (minZoom == 0 && minZoomByValue.isEmpty()) {
      return null;
    }

    Function<SourceFeature, Byte> staticZooms =
      sf -> (byte) Math.max(minZoom,
        minZoomFromTilePercent(sf, minTilePercent));

    if (minZoomByValue.isEmpty()) {
      return (sf, key) -> staticZooms.apply(sf);
    }

    //Attribute value-specific zooms override static zooms
    return (sourceFeature, key) -> minZoomByValue.getOrDefault(key, staticZooms.apply(sourceFeature));
  }

  private static byte minZoomFromTilePercent(SourceFeature sf, Double minTilePercent) {
    if (minTilePercent == null) {
      return 0;
    }
    try {
      return (byte) (Math.log(minTilePercent / sf.area()) / LOG4);
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
    attributeMinZoom = attributeMinZoom == null ? 0 : attributeMinZoom.byteValue();

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
        attrExcludeWhen == null ? Expression.TRUE : Expression.not(matcher(attrExcludeWhen, tagValueProducer))
      );

    var minTileCoverage = attrIncludeWhen == null ? null : attribute.minTileCoverSize();

    BiFunction<SourceFeature, Object, Byte> attributeZoomProducer =
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
   * Generates a factory method which creates a {@link Feature} from a {@link FeatureCollector} of the appropriate
   * geometry type.
   * 
   * @param layerName - name of the layer
   * @param type      - type of geometry
   * @return geometry factory method
   */
  private Function<FeatureCollector, Feature> geometryMapFeature(String layerName, GeometryType type) {
    return switch (type) {
      case POLYGON -> fc -> fc.polygon(layerName);
      case LINE -> fc -> fc.line(layerName);
      case POINT -> fc -> fc.point(layerName);
      default -> throw new IllegalArgumentException("Unhandled geometry type " + type);
    };
  }

  /**
   * Generates a test for whether a source feature is of the correct geometry to be included in the tile.
   * 
   * @param type type of geometry
   * @return geometry test method
   */
  private static Predicate<SourceFeature> geometryTest(GeometryType type) {
    return switch (type) {
      case POLYGON -> SourceFeature::canBePolygon;
      case LINE -> SourceFeature::canBeLine;
      case POINT -> SourceFeature::isPoint;
      default -> throw new IllegalArgumentException("Unhandled geometry type " + type);
    };
  }

  /**
   * Determines whether to include a source feature in the tiles.
   * 
   * @param sourceFeature - source feature
   * @return true if the feature will be rendered
   */
  public boolean includeWhen(SourceFeature sourceFeature) {
    //Is this from the right source?
    if (!sources.contains(sourceFeature.getSource())) {
      return false;
    }

    //Is this the right type of geometry?
    if (!geometryTest.test(sourceFeature)) {
      return false;
    }

    //Check for matching tags
    return tagTest.evaluate(sourceFeature);
  }

  /**
   * Generates a tile feature based on a source feature.
   * 
   * @param sourceFeature - input source feature
   * @param features      - output rendered feature collector
   */
  public void processFeature(SourceFeature sourceFeature, FeatureCollector features) {

    var minZoom = zoomOverride.getOrElse(sourceFeature, featureMinZoom);

    Feature f = geometryFactory.apply(features)
      .setMinZoom(minZoom);
    if (featureMaxZoom != null) {
      f.setMaxZoom(featureMaxZoom);
    }
    attributeProcessors.forEach(p -> p.accept(sourceFeature, f));
  }
}
