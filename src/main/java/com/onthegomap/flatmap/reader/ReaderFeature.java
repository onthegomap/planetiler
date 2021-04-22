package com.onthegomap.flatmap.reader;

import com.onthegomap.flatmap.SourceFeature;
import org.locationtech.jts.geom.Geometry;

public class ReaderFeature implements SourceFeature {

  private final Geometry geometry;

  public ReaderFeature(Geometry geometry) {
    this.geometry = geometry;
  }

  @Override
  public Geometry getGeometry() {
    return geometry;
  }
}
