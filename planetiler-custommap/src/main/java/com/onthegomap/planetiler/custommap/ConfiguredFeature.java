package com.onthegomap.planetiler.custommap;

import com.onthegomap.planetiler.FeatureCollector;
import com.onthegomap.planetiler.FeatureCollector.Feature;
import com.onthegomap.planetiler.custommap.configschema.AttributeDataType;
import com.onthegomap.planetiler.custommap.configschema.AttributeDefinition;
import com.onthegomap.planetiler.custommap.configschema.FeatureCriteria;
import com.onthegomap.planetiler.custommap.configschema.FeatureGeometryType;
import com.onthegomap.planetiler.custommap.configschema.FeatureItem;
import com.onthegomap.planetiler.custommap.configschema.TagCriteria;
import com.onthegomap.planetiler.geo.GeometryException;
import com.onthegomap.planetiler.reader.SourceFeature;
import com.onthegomap.planetiler.util.ZoomFunction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Predicate;

public class ConfiguredFeature {

  private Collection<String> sources;
  private Predicate<SourceFeature> geometryTest;
  private Function<FeatureCollector, Feature> geometryFactory;
  private Predicate<SourceFeature> tagTest;

  private static final double BUFFER_SIZE = 4.0;
  private static final double LOG4 = Math.log(4);

  private List<BiConsumer<SourceFeature, Feature>> attributeProcessors = new ArrayList<>();

  public ConfiguredFeature(String layerName, FeatureItem feature) {
    sources = feature.getSources();
    FeatureCriteria includeWhen = feature.getIncludeWhen();

    FeatureGeometryType geometryType = includeWhen.getGeometry();

    geometryTest = geometryTest(geometryType);
    geometryFactory = geometryMapFeature(layerName, geometryType);

    tagTest = tagTest(includeWhen.getTag());

    feature.getAttributes().forEach(attribute -> {
      attributeProcessors.add(attributeProcessor(attribute));
    });
  }

  static Function<SourceFeature, Object> attributeValueProducer(AttributeDefinition attribute) {

    String constVal = attribute.getConstantValue();
    if (constVal != null) {
      return sf -> constVal;
    }

    String tagVal = attribute.getTagValue();
    if (tagVal != null) {
      Function<Object, Object> typeConverter = typeConverter(attribute.getDataType());
      return sf -> typeConverter.apply(sf.getTag(tagVal, null));
    }
    throw new IllegalArgumentException("No value producer specified");
  }

  private static Function<Object, Object> typeConverter(AttributeDataType attributeDataType) {

    if (Objects.isNull(attributeDataType)) {
      return in -> {
        return in;
      };
    }

    switch (attributeDataType) {
      case bool:
        return booleanTypeConverter();
      //Default: pass through
      default:
        return in -> in;
    }
  }

  private static Set<String> booleanTrue = new HashSet<>(Arrays.<String>asList("true", "yes", "1"));
  private static Set<String> booleanFalse = new HashSet<>(Arrays.<String>asList("false", "no", "0"));

  private static Function<Object, Object> booleanTypeConverter() {
    return s -> {
      if (booleanTrue.contains(s)) {
        return Boolean.TRUE;
      }
      if (booleanFalse.contains(s)) {
        return Boolean.FALSE;
      }
      return null;
    };
  }

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

  private static BiConsumer<SourceFeature, Feature> attributeProcessor(AttributeDefinition attribute) {
    String tagKey = attribute.getKey();
    Integer configuredMinZoom = attribute.getMinZoom();

    int minZoom = configuredMinZoom == null ? 0 : configuredMinZoom.intValue();
    Function<SourceFeature, Object> attributeValueProducer = attributeValueProducer(attribute);

    FeatureCriteria attrIncludeWhen = attribute.getIncludeWhen();
    FeatureCriteria attrExcludeWhen = attribute.getExcludeWhen();

    Predicate<SourceFeature> attributeTest =
      attrIncludeWhen == null ? sf -> true : attributeTagTest(attrIncludeWhen, attributeValueProducer);
    Predicate<SourceFeature> attributeExcludeTest =
      attrExcludeWhen == null ? sf -> true : attributeTagTest(attrExcludeWhen, attributeValueProducer).negate();

    Double minTileCoverage = attrIncludeWhen == null ? null : attrIncludeWhen.getMinTileCoverSize();

    Function<SourceFeature, Integer> attributeZoomProducer;

    if (minTileCoverage != null && minTileCoverage > 0.0) {
      attributeZoomProducer = attributeZoomThreshold(minTileCoverage, minZoom);
    } else {
      attributeZoomProducer = sf -> minZoom;
    }

    return (sf, f) -> {
      if (attributeTest.test(sf) && attributeExcludeTest.test(sf)) {
        f.setAttrWithMinzoom(tagKey, attributeValueProducer.apply(sf), attributeZoomProducer.apply(sf));
      }
    };
  }

  private static Function<FeatureCollector, Feature> geometryMapFeature(String layerName, FeatureGeometryType type) {
    switch (type) {
      case polygon:
        return fc -> fc.polygon(layerName);
      case linestring:
        return fc -> fc.line(layerName);
      case point:
        return fc -> fc.point(layerName);
      default:
        throw new IllegalArgumentException("Unhandled geometry type " + type);
    }
  }

  private static Predicate<SourceFeature> geometryTest(FeatureGeometryType type) {
    switch (type) {
      case polygon:
        return sf -> sf.canBePolygon();
      case linestring:
        return sf -> sf.canBeLine();
      case point:
        return sf -> sf.isPoint();
      default:
        throw new IllegalArgumentException("Unhandled geometry type " + type);
    }
  }

  static Predicate<SourceFeature> attributeTagTest(FeatureCriteria attrIncludeWhen,
    Function<SourceFeature, Object> attributeValueProducer) {

    List<Predicate<SourceFeature>> conditions = new ArrayList<>();

    TagCriteria tagCriteria = attrIncludeWhen.getTag();

    if (tagCriteria != null) {

      String keyTest = tagCriteria.getKey();
      Collection<String> valTest = tagCriteria.getValue();

      if (keyTest == null) {
        conditions.add(sf -> true);
      } else {
        conditions.add(sf -> {
          if (sf.hasTag(keyTest)) {
            return valTest.contains(sf.getTag(keyTest));
          }
          return false;
        });
      }

    }

    if (conditions.isEmpty()) {
      return sf -> true;
    }

    Predicate<SourceFeature> test = conditions.remove(0);
    for (Predicate<SourceFeature> condition : conditions) {
      test = test.and(condition);
    }

    return test;
  }

  private static Predicate<SourceFeature> tagTest(TagCriteria tagCriteria) {
    if (tagCriteria == null) {
      return sf -> true;
    }
    String keyTest = tagCriteria.getKey();
    Collection<String> valTest = tagCriteria.getValue();
    if (keyTest == null) {
      return sf -> true;
    }
    return tagTest(keyTest, valTest);
  }

  private static Predicate<SourceFeature> tagTest(String key, Collection<String> values) {
    return sf -> {
      if (sf.hasTag(key)) {
        return values.contains(sf.getTag(key).toString());
      }
      return false;
    };
  }

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

  public void processFeature(SourceFeature sourceFeature, FeatureCollector features) {

    Feature f = geometryFactory.apply(features);

    f.setBufferPixels(BUFFER_SIZE)
      // and also whenever you set a label grid size limit, make sure you increase the buffer size so no
      // label grid squares will be the consistent between adjacent tiles
      .setBufferPixelOverrides(ZoomFunction.maxZoom(12, 32));

    attributeProcessors.forEach(p -> p.accept(sourceFeature, f));
  }
}
