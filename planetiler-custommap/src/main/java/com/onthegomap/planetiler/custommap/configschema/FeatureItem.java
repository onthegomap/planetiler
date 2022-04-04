package com.onthegomap.planetiler.custommap.configschema;

import java.util.Collection;

public class FeatureItem {
  private Collection<String> sources;
  private int[] zoom;
  private FeatureCriteria includeWhen;
  private FeatureCriteria excludeWhen;
  private Collection<AttributeDefinition> attributes;

  public Collection<String> getSources() {
    return sources;
  }

  public void setSources(Collection<String> sources) {
    this.sources = sources;
  }

  public int[] getZoom() {
    return zoom;
  }

  public void setZoom(int[] zoom) {
    this.zoom = zoom;
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

  public Collection<AttributeDefinition> getAttributes() {
    return attributes;
  }

  public void setAttributes(Collection<AttributeDefinition> attributes) {
    this.attributes = attributes;
  }
}
