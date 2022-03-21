package com.onthegomap.planetiler.custommap;

import java.util.Objects;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Predicate;

import com.onthegomap.planetiler.FeatureCollector.Feature;
import com.onthegomap.planetiler.reader.SourceFeature;

public class ValueParser {
  private static final Set<String> booleanTrueValues = Set.of("1", "true", "yes");

  public static BiConsumer<SourceFeature, Feature> passBoolAttrIfTrue(String tagName, int minZoom) {
    return passAttrOnCondition(tagName, minZoom, source -> {
      Object value = source.getTag(tagName);
      return Objects.nonNull(value) && booleanTrueValues.contains(value);
    });
  }

  public static BiConsumer<SourceFeature, Feature> passAttrOnCondition(String tagName, int minZoom,
    Predicate<SourceFeature> condition) {
    return (source, dest) -> {
      if (condition.test(source)) {
        dest.setAttrWithMinzoom(tagName, source.getTag(tagName), minZoom);
      }
    };
  }
}
