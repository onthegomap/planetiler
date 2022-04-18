package com.onthegomap.planetiler.custommap;

import com.onthegomap.planetiler.FeatureCollector;
import com.onthegomap.planetiler.FeatureCollector.Feature;
import com.onthegomap.planetiler.custommap.configschema.AttributeDefinition;
import com.onthegomap.planetiler.custommap.configschema.FeatureItem;
import com.onthegomap.planetiler.custommap.configschema.TagCriteria;
import com.onthegomap.planetiler.custommap.configschema.ZoomConfig;
import com.onthegomap.planetiler.custommap.configschema.ZoomFilter;
import com.onthegomap.planetiler.geo.GeometryException;
import com.onthegomap.planetiler.geo.GeometryType;
import com.onthegomap.planetiler.reader.SourceFeature;
import java.util.ArrayList;
import java.util.Collection;
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
  private final Predicate<SourceFeature> tagTest;
  private final BiConsumer<SourceFeature, Feature> zoomConfig;
  private final TagValueProducer tagValueProducer;

  private static final double LOG4 = Math.log(4);
  private static final BiConsumer<SourceFeature, Feature> ALLOW_ALL_ZOOMS = (sf, f) -> {
  };

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
      .orElse(sf -> true);

    //Test to determine at which zooms to include this feature based on tagging
    zoomConfig = zoomFilter(feature.zoom());

    //Factory to generate the right feature type from FeatureCollector
    geometryFactory = geometryMapFeature(layerName, geometryType);

    //Configure logic for each attribute in the output tile
    feature.attributes().forEach(attribute -> {
      attributeProcessors.add(attributeProcessor(attribute));
    });
  }

  /**
   * Generate zoom-based inclusion logic based on tagging and overall zoom limits. Tag-based zoom logic will override
   * universal zoom logic. The returned lambda will configure zoom limits for a specified {@link SourceFeature} by
   * invoking the appropriate methods in {@link Feature}.
   * 
   * @param zoomConfig zoom configuration
   * @return processing logic
   */
  private BiConsumer<SourceFeature, Feature> zoomFilter(ZoomConfig zoomConfig) {

    if (zoomConfig == null) {
      return ALLOW_ALL_ZOOMS;
    }

    Collection<ZoomFilter> zfList = zoomConfig.zoomFilter();
    if (zfList == null || zfList.isEmpty()) {
      return ALLOW_ALL_ZOOMS;
    }

    return (sf, f) -> {
      Optional<ZoomFilter> zoomFilterMatch = zfList
        .stream()
        .filter(zf -> zf.tag().matcher(tagValueProducer).test(sf))
        .findFirst();

      if (zoomFilterMatch.isPresent()) {
        ZoomFilter zf = zoomFilterMatch.get();
        f.setMinZoom(zf.minZoom());
        f.setMaxZoom(zoomConfig.maxZoom());
      } else {
        f.setMinZoom(zoomConfig.minZoom());
        f.setMaxZoom(zoomConfig.maxZoom());
      }
    };
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
  private Function<SourceFeature, Object> attributeValueProducer(AttributeDefinition attribute) {

    Object constVal = attribute.constantValue();
    if (constVal != null) {
      return sf -> constVal;
    }

    String tagVal = attribute.tagValue();
    if (tagVal != null) {
      return tagValueProducer.getValueProducer(tagVal);
    }
    throw new IllegalArgumentException("No value producer specified");
  }

  /**
   * Generate logic which determines the minimum zoom level for a feature based on a configured pixel size limit.
   * 
   * @param minTilePercent - minimum percentage of a tile that a feature must cover to be shown
   * @param minZoom        - global minimum zoom for this feature
   * @return minimum zoom function
   */
  private static Function<SourceFeature, Integer> attributeZoomThreshold(double minTilePercent, int minZoom) {
    return sf -> {
      try {
        int zoom = (int) (Math.log(minTilePercent / sf.area()) / LOG4);
        return Math.max(minZoom, Math.min(zoom, 14));
      } catch (GeometryException e) {
        return 14;
      }
    };
  };

  /**
   * Generates a function which produces a fully-configured attribute for a feature.
   * 
   * @param attribute - configuration for this attribute
   * @return processing logic
   */
  private BiConsumer<SourceFeature, Feature> attributeProcessor(AttributeDefinition attribute) {
    String tagKey = attribute.key();
    Integer configuredMinZoom = attribute.minZoom();

    int minZoom = configuredMinZoom == null ? 0 : configuredMinZoom.intValue();
    Function<SourceFeature, Object> attributeValueProducer = attributeValueProducer(attribute);

    TagCriteria attrIncludeWhen = attribute.includeWhen();
    TagCriteria attrExcludeWhen = attribute.excludeWhen();

    Predicate<SourceFeature> attributeTest =
      attrIncludeWhen == null ? sf -> true : attrIncludeWhen.matcher(tagValueProducer);
    Predicate<SourceFeature> attributeExcludeTest =
      attrExcludeWhen == null ? sf -> true : attrExcludeWhen.matcher(tagValueProducer).negate();

    Double minTileCoverage = attrIncludeWhen == null ? null : attribute.minTileCoverSize();

    Function<SourceFeature, Integer> attributeZoomProducer;

    if (minTileCoverage != null && minTileCoverage > 0.0) {
      attributeZoomProducer = attributeZoomThreshold(minTileCoverage, minZoom);
    } else {
      attributeZoomProducer = sf -> minZoom;
    }

    if (minZoom > 0) {
      return (sf, f) -> {
        if (attributeTest.test(sf) && attributeExcludeTest.test(sf)) {
          f.setAttrWithMinzoom(tagKey, attributeValueProducer.apply(sf), attributeZoomProducer.apply(sf));
        }
      };
    }

    return (sf, f) -> {
      if (attributeTest.test(sf) && attributeExcludeTest.test(sf)) {
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
  private static Function<FeatureCollector, Feature> geometryMapFeature(String layerName, GeometryType type) {
    switch (type) {
      case POLYGON:
        return fc -> fc.polygon(layerName);
      case LINE:
        return fc -> fc.line(layerName);
      case POINT:
        return fc -> fc.point(layerName);
      default:
        throw new IllegalArgumentException("Unhandled geometry type " + type);
    }
  }

  /**
   * Generate a test for whether a source feature is of the correct geometry to be included in the tile.
   * 
   * @param type type of geometry
   * @return geometry test method
   */
  private static Predicate<SourceFeature> geometryTest(GeometryType type) {
    switch (type) {
      case POLYGON:
        return sf -> sf.canBePolygon();
      case LINE:
        return sf -> sf.canBeLine();
      case POINT:
        return sf -> sf.isPoint();
      default:
        throw new IllegalArgumentException("Unhandled geometry type " + type);
    }
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
    return tagTest.test(sourceFeature);
  }

  /**
   * Generate a tile feature based on a source feature
   * 
   * @param sourceFeature - input source feature
   * @param features      - output rendered feature collector
   */
  public void processFeature(SourceFeature sourceFeature, FeatureCollector features) {
    Feature f = geometryFactory.apply(features);
    zoomConfig.accept(sourceFeature, f);
    attributeProcessors.forEach(p -> p.accept(sourceFeature, f));
  }
}
