package com.onthegomap.planetiler.custommap.configschema;

import java.util.Collection;

public class ZoomConfig {
  private Byte minZoom;
  private Byte maxZoom;
  private Collection<ZoomFilter> zoomFilter;

  public Byte getMinZoom() {
    return minZoom;
  }

  public void setMinZoom(Byte minZoom) {
    this.minZoom = minZoom;
  }

  public Byte getMaxZoom() {
    return maxZoom;
  }

  public void setMaxZoom(Byte maxZoom) {
    this.maxZoom = maxZoom;
  }

  public Collection<ZoomFilter> getZoomFilter() {
    return zoomFilter;
  }

  public void setZoomFilter(Collection<ZoomFilter> zoomFilter) {
    this.zoomFilter = zoomFilter;
  }
}
