package com.onthegomap.planetiler.custommap.configschema;

public class ZoomConfig {
  private byte minZoom;
  private byte maxZoom;

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
}
