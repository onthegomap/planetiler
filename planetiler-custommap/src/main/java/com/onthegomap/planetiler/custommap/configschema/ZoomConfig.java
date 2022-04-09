package com.onthegomap.planetiler.custommap.configschema;

import java.util.Collection;

public class ZoomConfig {
  private byte minZoom;
  private byte maxZoom;
  private Collection<ZoomFilter> zoomFilter;

  public byte getMinZoom() {
    return minZoom;
  }

  public void setMinZoom(byte minZoom) {
    this.minZoom = minZoom;
  }

  public byte getMaxZoom() {
    return maxZoom;
  }

  public void setMaxZoom(byte maxZoom) {
    this.maxZoom = maxZoom;
  }

  public Collection<ZoomFilter> getZoomFilter() {
    return zoomFilter;
  }

  public void setZoomFilter(Collection<ZoomFilter> zoomFilter) {
    this.zoomFilter = zoomFilter;
  }
}
