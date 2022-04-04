package com.onthegomap.planetiler.custommap;

import java.util.Collections;
import java.util.Map;

import org.locationtech.jts.geom.Geometry;

import com.onthegomap.planetiler.geo.GeometryException;
import com.onthegomap.planetiler.reader.SourceFeature;

public class TestAreaSourceFeature extends SourceFeature {

  public TestAreaSourceFeature(Map<String, Object> tags, String source, String sourceLayer) {
    super(tags, source, sourceLayer, Collections.emptyList(), 0);
  }

  @Override
  public Geometry latLonGeometry() throws GeometryException {
    return null;
  }

  @Override
  public Geometry worldGeometry() throws GeometryException {
    return null;
  }

  @Override
  public boolean isPoint() {
    return false;
  }

  @Override
  public boolean canBePolygon() {
    return true;
  }

  @Override
  public boolean canBeLine() {
    return false;
  }

}
