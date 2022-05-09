package com.onthegomap.planetiler.custommap;

import com.onthegomap.planetiler.FeatureCollector;
import com.onthegomap.planetiler.FeatureCollector.Feature;
import com.onthegomap.planetiler.custommap.configschema.AttributeDefinition;
import com.onthegomap.planetiler.custommap.configschema.FeatureItem;
import com.onthegomap.planetiler.custommap.configschema.ZoomConfig;
import com.onthegomap.planetiler.custommap.configschema.ZoomFilter;
import com.onthegomap.planetiler.expression.Expression;
import com.onthegomap.planetiler.expression.MultiExpression;
import com.onthegomap.planetiler.expression.MultiExpression.Index;
import com.onthegomap.planetiler.geo.GeometryException;
import com.onthegomap.planetiler.geo.GeometryType;
import com.onthegomap.planetiler.reader.SourceFeature;
import com.onthegomap.planetiler.reader.WithTags;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.BiConsumer;
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
  private final Index<Byte> zoomTagConfig;
  private final Byte featureMinZoom;
  private final Byte featureMaxZoom;
  private final TagValueProducer tagValueProducer;

  private static final double LOG4 = Math.log(4);

  private List<BiConsumer<SourceFeature, Feature>> attributeProcessors = new ArrayList<>();

  public ConfiguredFeature(String layerName, TagValueProducer tagValueProducer, FeatureItem feature) {
    sources = feature.sources();

    GeometryType geometryType = feature.geometry();

    //Test to determine whether this type of geometry is included
    geometryTest = geometryTest(geometryType);

    //Factory to treat OSM tag values as specific data type values
    this.tagValueProducer = tagValueProducer;

    //Test to determine whether this feature is included based on tagging
    tagTest = Optional.ofNullable(feature.includeWhen())
      .filter(Objects::nonNull)
      .map(tc -> tc.matcher(tagValueProducer))
      .orElse(Expression.TRUE);

    //Test to determine at which zooms to include this feature based on tagging
    zoomTagConfig = zoomFilter(feature.zoom());
    if (feature.zoom() != null) {
      featureMinZoom = feature.zoom().minZoom();
      featureMaxZoom = feature.zoom().maxZoom();
    } else {
      featureMinZoom = 0;
      featureMaxZoom = null;
    }

    //Factory to generate the right feature type from FeatureCollector
    geometryFactory = geometryMapFeature(layerName, geometryType);

    //Configure logic for each attribute in the output tile
    feature.attributes()
      .stream()
      .map(this::attributeProcessor)
      .forEach(attributeProcessors::add);
  }

  /**
   * Generate zoom-based inclusion logic based on tagging and overall zoom limits. Tag-based zoom logic will override
   * universal zoom logic. The returned lambda will configure zoom limits for a specified {@link SourceFeature} by
   * invoking the appropriate methods in {@link Feature}.
   * 
   * @param zoomConfig zoom configuration
   * @return index of zoom levels, or null if unrestricted
   */
  private Index<Byte> zoomFilter(ZoomConfig zoomConfig) {

    if (zoomConfig == null) {
      return null;
    }

    Collection<ZoomFilter> zfList = zoomConfig.zoomFilter();
    if (zfList == null || zfList.isEmpty()) {
      return null;
    }

    return MultiExpression.of(zfList
      .stream()
      .map(zf -> MultiExpression.entry(zf.minZoom(), zf.tag().matcher(tagValueProducer)))
      .toList())
      .index();
  }

  /**
   * Produce logic that generates attribute values based on configuration and input data. If both a constantValue
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
   * @param zoomTagConfig
   * @return minimum zoom function
   */
  private static Function<SourceFeature, Integer> attributeZoomThreshold(Double minTilePercent, int minZoom) {

    if (minZoom == 0) {
      return null;
    }

    return sf -> Math.max(minZoom,
      minZoomFromTilePercent(sf, minTilePercent));
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
   * Determine the minimum zoom of a feature, based on its tagging.
   * 
   * @param sf           the source feature
   * @param zoomConfig   tag-based zoom configuration index from JSON
   * @param layerMinZoom overall minimum zoom for this type of feature
   * @return minimum zoom level for this feature
   */
  private static int minZoomFromFeatureTagging(SourceFeature sf, Index<Byte> zoomConfig, Byte layerMinZoom) {
    if (zoomConfig == null) {
      return 0;
    }
    var zoomMatches = zoomConfig.getMatches(sf);
    if (zoomMatches.isEmpty()) {
      return 0;
    }
    return Math.max(layerMinZoom, Collections.min(zoomMatches));
  }

  /**
   * Generates a function which produces a fully-configured attribute for a feature.
   * 
   * @param attribute - configuration for this attribute
   * @return processing logic
   */
  private BiConsumer<SourceFeature, Feature> attributeProcessor(AttributeDefinition attribute) {
    var tagKey = attribute.key();
    Integer configuredMinZoom = attribute.minZoom();

    var attributeMinZoom = configuredMinZoom == null ? 0 : configuredMinZoom.intValue();
    var attributeValueProducer = attributeValueProducer(attribute);

    var attrIncludeWhen = attribute.includeWhen();
    var attrExcludeWhen = attribute.excludeWhen();

    var attributeTest =
      Expression.and(
        attrIncludeWhen == null ? Expression.TRUE : attrIncludeWhen.matcher(tagValueProducer),
        attrExcludeWhen == null ? Expression.TRUE : Expression.not(attrExcludeWhen.matcher(tagValueProducer))
      );

    var minTileCoverage = attrIncludeWhen == null ? null : attribute.minTileCoverSize();
    Function<SourceFeature, Integer> attributeZoomProducer;

    attributeZoomProducer = attributeZoomThreshold(minTileCoverage, attributeMinZoom);

    if (attributeZoomProducer != null) {
      return (sf, f) -> {
        if (attributeTest.evaluate(sf)) {
          f.setAttrWithMinzoom(tagKey, attributeValueProducer.apply(sf), attributeZoomProducer.apply(sf));
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
   * Generate a test for whether a source feature is of the correct geometry to be included in the tile.
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
   * Test to determine whether to include a source feature in the tiles
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
    Feature f = geometryFactory.apply(features)
      .setMinZoom(minZoomFromFeatureTagging(sourceFeature, zoomTagConfig, featureMinZoom));
    if (featureMaxZoom != null) {
      f.setMaxZoom(featureMaxZoom);
    }
    attributeProcessors.forEach(p -> p.accept(sourceFeature, f));
  }
}
