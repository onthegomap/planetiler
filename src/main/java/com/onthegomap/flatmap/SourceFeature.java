package com.onthegomap.flatmap;

import com.onthegomap.flatmap.geo.GeoUtils;
import com.onthegomap.flatmap.geo.GeometryException;
import java.util.Map;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Lineal;

public abstract class SourceFeature {

  private final Map<String, Object> properties;
  private final String source;
  private final String sourceLayer;

  protected SourceFeature(Map<String, Object> properties, String source, String sourceLayer) {
    this.properties = properties;
    this.source = source;
    this.sourceLayer = sourceLayer;
  }

  public abstract Geometry latLonGeometry() throws GeometryException;

  public abstract Geometry worldGeometry() throws GeometryException;

  public void setTag(String key, Object value) {
    properties.put(key, value);
  }

  public Map<String, Object> properties() {
    return properties;
  }

  private Geometry centroid = null;

  public Geometry centroid() throws GeometryException {
    return centroid != null ? centroid : (centroid =
      canBePolygon() ? polygon().getCentroid() :
        canBeLine() ? line().getCentroid() :
          worldGeometry().getCentroid());
  }

  private Geometry pointOnSurface = null;

  public Geometry pointOnSurface() throws GeometryException {
    return pointOnSurface != null ? pointOnSurface : (pointOnSurface =
      canBePolygon() ? polygon().getInteriorPoint() :
        canBeLine() ? line().getInteriorPoint() :
          worldGeometry().getInteriorPoint());
  }

  private Geometry linearGeometry = null;

  protected Geometry computeLine() throws GeometryException {
    Geometry world = worldGeometry();
    return world instanceof Lineal ? world : GeoUtils.polygonToLineString(world);
  }

  public final Geometry line() throws GeometryException {
    if (!canBeLine()) {
      throw new GeometryException("cannot be line");
    }
    if (linearGeometry == null) {
      linearGeometry = computeLine();
    }
    return linearGeometry;
  }


  private Geometry polygonGeometry = null;

  protected Geometry computePolygon() throws GeometryException {
    return worldGeometry();
  }

  public final Geometry polygon() throws GeometryException {
    if (!canBePolygon()) {
      throw new GeometryException("cannot be polygon");
    }
    return polygonGeometry != null ? polygonGeometry : (polygonGeometry = computePolygon());
  }

  private double area = Double.NaN;

  public double area() throws GeometryException {
    return Double.isNaN(area) ? (area = canBePolygon() ? polygon().getArea() : 0) : area;
  }

  private double length = Double.NaN;

  public double length() throws GeometryException {
    return Double.isNaN(length) ? (length =
      (isPoint() || canBePolygon() || canBeLine()) ? worldGeometry().getLength() : 0) : length;
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

  public abstract boolean canBePolygon();

  public abstract boolean canBeLine();

  public String getSource() {
    return source;
  }

  public String getSourceLayer() {
    return sourceLayer;
  }
}
