package com.onthegomap.planetiler.custommap.configschema;

import com.onthegomap.planetiler.reader.SourceFeature;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class AttributeDefinition {
  private String key;
  private Object constantValue;
  private String tagValue;
  private TagCriteria includeWhen;
  private TagCriteria excludeWhen;
  private int minZoom;
  private Double minTileCoverSize;

  private BiFunction<SourceFeature, String, Object> tagDataSupplier = (feature, key) -> feature.getTag(key);

  public String getKey() {
    return key;
  }

  public void setKey(String key) {
    this.key = key;
  }

  public Object getConstantValue() {
    return constantValue;
  }

  public void setConstantValue(Object constantValue) {
    this.constantValue = constantValue;
  }

  public String getTagValue() {
    return tagValue;
  }

  public void setTagValue(String tagValue) {
    this.tagValue = tagValue;
  }

  public TagCriteria getIncludeWhen() {
    return includeWhen;
  }

  public void setIncludeWhen(TagCriteria includeWhen) {
    this.includeWhen = includeWhen;
  }

  public TagCriteria getExcludeWhen() {
    return excludeWhen;
  }

  public void setExcludeWhen(TagCriteria excludeWhen) {
    this.excludeWhen = excludeWhen;
  }

  public int getMinZoom() {
    return minZoom;
  }

  public void setMinZoom(int minZoom) {
    this.minZoom = minZoom;
  }

  public Double getMinTileCoverSize() {
    return minTileCoverSize;
  }

  public void setMinTileCoverSize(Double minTileCoverSize) {
    this.minTileCoverSize = minTileCoverSize;
  }

  /**
   * Returns a function that determines whether a source feature matches any of the entries in this specification
   * 
   * @param sf source feature
   * @return a predicate which returns true if this criteria matches
   */
  public Predicate<SourceFeature> tagMatcher() {

    List<Predicate<SourceFeature>> tagTests = new ArrayList<>();

    if (includeWhen != null) {
      tagTests.addAll(tagCriteriaMatchers(includeWhen));
    }

    Predicate<SourceFeature> test;

    if (tagTests.isEmpty()) {
      test = sf -> true;
    } else {
      test = tagTests.remove(0);
      while (!tagTests.isEmpty()) {
        test = test.or(tagTests.remove(0));
      }
    }

    if (excludeWhen != null) {
      tagCriteriaMatchers(excludeWhen)
        .stream()
        .map(Predicate::negate)
        .forEach(tagTests::add);
    }

    while (!tagTests.isEmpty()) {
      test = test.and(tagTests.remove(0));
    }

    return test;
  }

  private List<Predicate<SourceFeature>> tagCriteriaMatchers(TagCriteria criteria) {
    return criteria.entrySet()
      .stream()
      .map(entry -> {
        if (entry.getValue() instanceof Collection) {
          Collection<?> values =
            (Collection<?>) entry.getValue();
          return (Predicate<SourceFeature>) sf -> values.contains(tagDataSupplier.apply(sf, entry.getKey()));
        } else {
          return (Predicate<SourceFeature>) sf -> entry.getValue().toString()
            .equals(tagDataSupplier.apply(sf, entry.getKey()));
        }
      })
      .collect(Collectors.toList());
  }
}
