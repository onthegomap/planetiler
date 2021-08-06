package com.onthegomap.flatmap.reader;

import com.onthegomap.flatmap.geo.GeoUtils;
import com.onthegomap.flatmap.geo.GeometryException;
import com.onthegomap.flatmap.reader.osm.OpenStreetMapReader;
import com.onthegomap.flatmap.util.Parse;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Lineal;
import org.locationtech.jts.geom.Polygon;

public abstract class SourceFeature {

  private final Map<String, Object> properties;
  private final String source;
  private final String sourceLayer;
  private final List<OpenStreetMapReader.RelationMember<OpenStreetMapReader.RelationInfo>> relationInfos;
  private final long id;

  protected SourceFeature(Map<String, Object> properties, String source, String sourceLayer,
    List<OpenStreetMapReader.RelationMember<OpenStreetMapReader.RelationInfo>> relationInfos, long id) {
    this.properties = properties;
    this.source = source;
    this.sourceLayer = sourceLayer;
    this.relationInfos = relationInfos;
    this.id = id;
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

  private Geometry centroidIfConvex = null;

  private Geometry computeCentroidIfConvex() throws GeometryException {
    if (!canBePolygon()) {
      return centroid();
    } else if (polygon() instanceof Polygon poly &&
      poly.getNumInteriorRing() == 0 &&
      GeoUtils.isConvex(poly.getExteriorRing())) {
      return centroid();
    } else {
      return pointOnSurface();
    }
  }

  public Geometry centroidIfConvex() throws GeometryException {
    return centroidIfConvex != null ? centroidIfConvex : (centroidIfConvex = computeCentroidIfConvex());
  }

  private Geometry linearGeometry = null;

  protected Geometry computeLine() throws GeometryException {
    Geometry world = worldGeometry();
    return world instanceof Lineal ? world : GeoUtils.polygonToLineString(world);
  }

  public final Geometry line() throws GeometryException {
    if (!canBeLine()) {
      throw new GeometryException("feature_not_line", "cannot be line");
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
      throw new GeometryException("feature_not_polygon", "cannot be polygon");
    }
    return polygonGeometry != null ? polygonGeometry : (polygonGeometry = computePolygon());
  }

  private Geometry validPolygon = null;

  private Geometry computeValidPolygon() throws GeometryException {
    Geometry polygon = polygon();
    if (!polygon.isValid()) {
      polygon = GeoUtils.fixPolygon(polygon);
    }
    return polygon;
  }

  public final Geometry validatedPolygon() throws GeometryException {
    if (!canBePolygon()) {
      throw new GeometryException("feature_not_polygon", "cannot be polygon");
    }
    return validPolygon != null ? validPolygon : (validPolygon = computeValidPolygon());
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

  public <T extends OpenStreetMapReader.RelationInfo> List<OpenStreetMapReader.RelationMember<T>> relationInfo(
    Class<T> relationInfoClass) {
    List<OpenStreetMapReader.RelationMember<T>> result = null;
    if (relationInfos != null) {
      for (OpenStreetMapReader.RelationMember<?> info : relationInfos) {
        if (relationInfoClass.isInstance(info.relation())) {
          if (result == null) {
            result = new ArrayList<>();
          }
          @SuppressWarnings("unchecked")
          OpenStreetMapReader.RelationMember<T> casted = (OpenStreetMapReader.RelationMember<T>) info;
          result.add(casted);
        }
      }
    }
    return result == null ? List.of() : result;
  }

  public final long id() {
    return id;
  }

  public String getString(String key) {
    Object value = getTag(key);
    return value == null ? null : value.toString();
  }

  public String getString(String key, String defaultValue) {
    Object value = getTag(key, defaultValue);
    return value == null ? null : value.toString();
  }

  public boolean getBoolean(String key) {
    return Parse.bool(getTag(key));
  }

  public long getLong(String key) {
    return Parse.parseLong(getTag(key));
  }

  public int getDirection(String key) {
    return Parse.direction(getTag(key));
  }

  public int getWayZorder() {
    return Parse.wayzorder(properties);
  }
}
