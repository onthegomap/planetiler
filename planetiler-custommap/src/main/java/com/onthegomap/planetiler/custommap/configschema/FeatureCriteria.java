package com.onthegomap.planetiler.custommap.configschema;

public class FeatureCriteria {
  private FeatureGeometryType geometry;
  private TagCriteria tag;
  private Double minTileCoverSize;

  public FeatureGeometryType getGeometry() {
    return geometry;
  }

  public void setGeometry(FeatureGeometryType geometry) {
    this.geometry = geometry;
  }

  public TagCriteria getTag() {
    return tag;
  }

  public void setTag(TagCriteria tag) {
    this.tag = tag;
  }

  public Double getMinTileCoverSize() {
    return minTileCoverSize;
  }

  public void setMinTileCoverSize(Double minTileCoverSize) {
    this.minTileCoverSize = minTileCoverSize;
  }
}
