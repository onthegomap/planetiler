package com.onthegomap.planetiler.custommap.configschema;

import java.util.Collection;

public class FeatureLayer {
  private String name;
  private Collection<FeatureItem> features;

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public Collection<FeatureItem> getFeatures() {
    return features;
  }

  public void setFeatures(Collection<FeatureItem> features) {
    this.features = features;
  }
}
