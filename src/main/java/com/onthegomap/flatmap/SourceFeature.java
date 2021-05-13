package com.onthegomap.flatmap;

import java.util.Map;
import org.locationtech.jts.geom.Geometry;

public abstract class SourceFeature {

  private final Map<String, Object> properties;

  protected SourceFeature(Map<String, Object> properties) {
    this.properties = properties;
  }

  public abstract Geometry latLonGeometry();

  public abstract Geometry worldGeometry();

  public void setTag(String key, Object value) {
    properties.put(key, value);
  }

  public Map<String, Object> properties() {
    return properties;
  }

  private Geometry centroid = null;

  public Geometry centroid() {
    return centroid != null ? centroid : (centroid = worldGeometry().getCentroid());
  }

  private Geometry linearGeometry = null;

  public Geometry line() {
    return linearGeometry != null ? linearGeometry : (linearGeometry = worldGeometry());
  }

  private Geometry polygonGeometry = null;

  public Geometry polygon() {
    return polygonGeometry != null ? polygonGeometry : (polygonGeometry = worldGeometry());
  }

  private double area = Double.NaN;

  public double area() {
    return Double.isNaN(area) ? (area = polygon().getArea()) : area;
  }

  private double length = Double.NaN;

  public double length() {
    return Double.isNaN(length) ? (length = line().getLength()) : length;
  }

  public Object getTag(String key) {
    return properties.get(key);
  }

  public Object getTag(String key, Object defaultValue) {
    Object val = properties.get(key);
    if (val == null) {
      return defaultValue;
    }
    return val;
  }

  public boolean hasTag(String key) {
    return properties.containsKey(key);
  }

  public boolean hasTag(String key, Object value) {
    return value.equals(getTag(key));
  }

  public boolean hasTag(String key, Object value, Object value2) {
    Object actual = getTag(key);
    return value.equals(actual) || value2.equals(actual);
  }

  public abstract boolean isPoint();
}
