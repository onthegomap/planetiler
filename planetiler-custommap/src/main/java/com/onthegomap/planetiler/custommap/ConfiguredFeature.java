package com.onthegomap.planetiler.custommap;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Predicate;

import com.onthegomap.planetiler.FeatureCollector;
import com.onthegomap.planetiler.FeatureCollector.Feature;
import com.onthegomap.planetiler.geo.GeometryException;
import com.onthegomap.planetiler.reader.SourceFeature;
import com.onthegomap.planetiler.util.ZoomFunction;

public class ConfiguredFeature implements CustomFeature {

  private Set<String> sources = new HashSet<>();
  private Predicate<SourceFeature> geometryTest;
  private Function<FeatureCollector, Feature> geometryFactory;
  private Predicate<SourceFeature> tagTest;
  private String layerName;
  private List<Object> attributes;

  private static final double BUFFER_SIZE = 4.0;
  private static final double LOG4 = Math.log(4);

  private List<BiConsumer<SourceFeature, Feature>> attributeProcessors = new ArrayList<>();

  public ConfiguredFeature(String layerName, Map<String, Object> featureDef) {
    this.layerName = layerName;
    sources = YamlParser.extractStringSet(featureDef.get("sources"));
    Map<String, Object> includeWhen = (Map<String, Object>) featureDef.get("includeWhen");

    String geometryType = YamlParser.getString(includeWhen, "geometry");

    geometryTest = geometryTest(geometryType);
    geometryFactory = geometryMapFeature(layerName, geometryType);

    tagTest = tagTest((Map<String, Object>) includeWhen.get("tag"));

    attributes = (List<Object>) featureDef.get("attributes");
    for (int i = 0; i < attributes.size(); i++) {
      attributeProcessors.add(attributeProcessor((Map<String, Object>) attributes.get(i)));
    }
  }

  private static Function<SourceFeature, Object> attributeValueProducer(Map<String, Object> map) {

    String constVal = YamlParser.getString(map, "constantValue");
    if (constVal != null) {
      return sf -> constVal;
    }

    String tagVal = YamlParser.getString(map, "tagValue");
    if (tagVal != null) {
      Function<Object, Object> typeConverter = typeConverter(YamlParser.getString(map, "dataType"));
      return sf -> typeConverter.apply(sf.getTag(tagVal, null));
    }
    throw new IllegalArgumentException("No value producer specified");
  }

  private static Function<Object, Object> typeConverter(String type) {

    if (Objects.isNull(type)) {
      return in -> {
        return in;
      };
    }

    switch (type) {
      case "boolean":
        return booleanTypeConverter();
    }

    //Default: pass through
    return in -> {
      return in;
    };
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

  private static BiConsumer<SourceFeature, Feature> attributeProcessor(Map<String, Object> map) {
    String tagKey = YamlParser.getString(map, "key");
    Long configuredMinZoom = YamlParser.getLong(map, "minZoom");

    int minZoom = configuredMinZoom == null ? 0 : configuredMinZoom.intValue();
    Function<SourceFeature, Object> attributeValueProducer = attributeValueProducer(map);

    Map<String, Object> attrIncludeWhen = (Map<String, Object>) map.get("includeWhen");
    Map<String, Object> attrExcludeWhen = (Map<String, Object>) map.get("excludeWhen");

    Predicate<SourceFeature> attributeTest = attributeTagTest(attrIncludeWhen, attributeValueProducer, true);
    Predicate<SourceFeature> attributeExcludeTest =
      attributeTagTest(attrExcludeWhen, attributeValueProducer, false).negate();

    Double minTileCoverage = YamlParser.getDouble(attrIncludeWhen, "minTileCoverSize");

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

  private static Function<FeatureCollector, Feature> geometryMapFeature(String layerName, String type) {
    switch (type) {
      case "polygon":
        return fc -> fc.polygon(layerName);
      case "linestring":
        return fc -> fc.line(layerName);
      case "point":
        return fc -> fc.point(layerName);
      default:
        throw new IllegalArgumentException("Unhandled geometry type " + type);
    }
  }

  private static Predicate<SourceFeature> geometryTest(String type) {
    switch (type) {
      case "polygon":
        return sf -> sf.canBePolygon();
      case "linestring":
        return sf -> sf.canBeLine();
      case "point":
        return sf -> sf.isPoint();
      default:
        throw new IllegalArgumentException("Unhandled geometry type " + type);
    }
  }

  private static Predicate<SourceFeature> attributeTagTest(Map<String, Object> tagConstraint,
    Function<SourceFeature, Object> attributeValueProducer, boolean defaultVal) {
    if (tagConstraint == null) {
      return sf -> defaultVal;
    }

    String keyTest = YamlParser.getString(tagConstraint, "key");
    Set<String> valTest = YamlParser.extractStringSet(tagConstraint.get("value"));
    if (keyTest == null) {
      return sf -> defaultVal;
    }
    return sf -> {
      if (sf.hasTag(keyTest)) {
        return valTest.contains(attributeValueProducer.apply(sf));
      }
      return false;
    };
  }

  private static Predicate<SourceFeature> tagTest(Map<String, Object> tagConstraint) {
    if (tagConstraint == null) {
      return sf -> true;
    }
    String keyTest = YamlParser.getString(tagConstraint, "key");
    Set<String> valTest = YamlParser.extractStringSet(tagConstraint.get("value"));
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

  @Override
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

  @Override
  public void processFeature(SourceFeature sourceFeature, FeatureCollector features) {

    Feature f = geometryFactory.apply(features);

    f.setBufferPixels(BUFFER_SIZE)
      // and also whenever you set a label grid size limit, make sure you increase the buffer size so no
      // label grid squares will be the consistent between adjacent tiles
      .setBufferPixelOverrides(ZoomFunction.maxZoom(12, 32));

    attributeProcessors.forEach(p -> p.accept(sourceFeature, f));
  }
}
