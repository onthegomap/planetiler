package com.onthegomap.planetiler.custommap.configschema;

public class ZoomFilter {
  private TagCriteria tag;
  private byte minZoom;

  public TagCriteria getTag() {
    return tag;
  }

  public void setTag(TagCriteria tag) {
    this.tag = tag;
  }

  public byte getMinZoom() {
    return minZoom;
  }

  public void setMinZoom(byte minZoom) {
    this.minZoom = minZoom;
  }
}
