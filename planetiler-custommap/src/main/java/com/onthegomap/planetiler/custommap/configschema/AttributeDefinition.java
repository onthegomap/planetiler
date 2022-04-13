package com.onthegomap.planetiler.custommap.configschema;

public class AttributeDefinition {
  private String key;
  private String constantValue;
  private String tagValue;
  private TagCriteria includeWhen;
  private TagCriteria excludeWhen;
  private int minZoom;
  private Double minTileCoverSize;

  public String getKey() {
    return key;
  }

  public void setKey(String key) {
    this.key = key;
  }

  public String getConstantValue() {
    return constantValue;
  }

  public void setConstantValue(String constantValue) {
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
}
