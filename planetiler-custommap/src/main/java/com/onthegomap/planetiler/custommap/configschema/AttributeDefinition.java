package com.onthegomap.planetiler.custommap.configschema;

public class AttributeDefinition {
  private String key;
  private String constantValue;
  private String tagValue;
  private AttributeDataType dataType;
  private FeatureCriteria includeWhen;
  private FeatureCriteria excludeWhen;
  private int minZoom;

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

  public AttributeDataType getDataType() {
    return dataType;
  }

  public void setDataType(AttributeDataType dataType) {
    this.dataType = dataType;
  }

  public FeatureCriteria getIncludeWhen() {
    return includeWhen;
  }

  public void setIncludeWhen(FeatureCriteria includeWhen) {
    this.includeWhen = includeWhen;
  }

  public FeatureCriteria getExcludeWhen() {
    return excludeWhen;
  }

  public void setExcludeWhen(FeatureCriteria excludeWhen) {
    this.excludeWhen = excludeWhen;
  }

  public int getMinZoom() {
    return minZoom;
  }

  public void setMinZoom(int minZoom) {
    this.minZoom = minZoom;
  }

}
