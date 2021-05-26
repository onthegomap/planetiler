package com.onthegomap.flatmap.read;

import com.onthegomap.flatmap.SourceFeature;
import com.onthegomap.flatmap.geo.GeoUtils;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Polygonal;
import org.locationtech.jts.geom.Puntal;

public class ReaderFeature extends SourceFeature {

  private final Geometry latLonGeometry;
  private final Map<String, Object> properties;

  public ReaderFeature(Geometry latLonGeometry, Map<String, Object> properties) {
    this(latLonGeometry, properties, null, null);
  }

  public ReaderFeature(Geometry latLonGeometry, Map<String, Object> properties, String source, String sourceLayer) {
    super(properties, source, sourceLayer);
    this.latLonGeometry = latLonGeometry;
    this.properties = properties;
  }

  public ReaderFeature(Geometry latLonGeometry, int numProperties, String source, String sourceLayer) {
    this(latLonGeometry, new HashMap<>(numProperties), source, sourceLayer);
  }

  @Override
  public Geometry latLonGeometry() {
    return latLonGeometry;
  }

  private Geometry worldGeometry;

  @Override
  public Geometry worldGeometry() {
    return worldGeometry != null ? worldGeometry
      : (worldGeometry = GeoUtils.ensureDescendingPolygonsSizes(GeoUtils.latLonToWorldCoords(latLonGeometry)));
  }

  public Map<String, Object> properties() {
    return properties;
  }

  @Override
  public boolean isPoint() {
    return latLonGeometry instanceof Puntal;
  }

  @Override
  public boolean canBePolygon() {
    return latLonGeometry instanceof Polygonal;
  }

  @Override
  public boolean canBeLine() {
    return !isPoint();
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == this) {
      return true;
    }
    if (obj == null || obj.getClass() != this.getClass()) {
      return false;
    }
    var that = (ReaderFeature) obj;
    return Objects.equals(this.latLonGeometry, that.latLonGeometry) &&
      Objects.equals(this.properties, that.properties);
  }

  @Override
  public int hashCode() {
    return Objects.hash(latLonGeometry, properties);
  }

  @Override
  public String toString() {
    return "ReaderFeature[" +
      "geometry=" + latLonGeometry + ", " +
      "properties=" + properties + ']';
  }
}
