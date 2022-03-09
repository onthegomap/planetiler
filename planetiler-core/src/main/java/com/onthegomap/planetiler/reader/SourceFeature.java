package com.onthegomap.planetiler.reader;

import com.onthegomap.planetiler.geo.GeoUtils;
import com.onthegomap.planetiler.geo.GeometryException;
import com.onthegomap.planetiler.reader.osm.OsmReader;
import com.onthegomap.planetiler.reader.osm.OsmRelationInfo;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.Lineal;
import org.locationtech.jts.geom.MultiLineString;
import org.locationtech.jts.geom.MultiPoint;
import org.locationtech.jts.geom.MultiPolygon;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;

/**
 * Base class for input features read from a data source.
 * <p>
 * Provides cached convenience methods with lazy initialization for geometric attributes derived from
 * {@link #latLonGeometry()} and {@link #worldGeometry()} to avoid computing them if not needed, and recomputing them if
 * needed by multiple features.
 * <p>
 * All geometries except for {@link #latLonGeometry()} return elements in world web mercator coordinates where (0,0) is
 * the northwest corner and (1,1) is the southeast corner of the planet.
 */
public abstract class SourceFeature implements WithTags {

  private final Map<String, Object> tags;
  private final String source;
  private final String sourceLayer;
  private final List<OsmReader.RelationMember<OsmRelationInfo>> relationInfos;
  private final long id;
  private Geometry centroid = null;
  private Geometry pointOnSurface = null;
  private Geometry centroidIfConvex = null;
  private Geometry linearGeometry = null;
  private Geometry polygonGeometry = null;
  private Geometry validPolygon = null;
  private double area = Double.NaN;
  private double length = Double.NaN;

  /**
   * Constructs a new input feature.
   *
   * @param tags          string key/value pairs associated with this element
   * @param source        source name that profile can use to distinguish between elements from different data sources
   * @param sourceLayer   layer name within {@code source} that profile can use to distinguish between different kinds
   *                      of elements in a given source.
   * @param relationInfos relations that this element is contained within
   * @param id            numeric ID of this feature within this source (i.e. an OSM element ID)
   */
  protected SourceFeature(Map<String, Object> tags, String source, String sourceLayer,
    List<OsmReader.RelationMember<OsmRelationInfo>> relationInfos, long id) {
    this.tags = tags;
    this.source = source;
    this.sourceLayer = sourceLayer;
    this.relationInfos = relationInfos;
    this.id = id;
  }

  // slight optimization: replace default implementation with direct access to the tags
  // map to get slightly improved performance when matching elements against expressions

  @Override
  public Object getTag(String key) {
    return tags.get(key);
  }

  @Override
  public boolean hasTag(String key) {
    return tags.containsKey(key);
  }


  @Override
  public Object getTag(String key, Object defaultValue) {
    Object val = tags.get(key);
    if (val == null) {
      return defaultValue;
    }
    return val;
  }

  @Override
  public Map<String, Object> tags() {
    return tags;
  }

  /**
   * Returns this feature's geometry in latitude/longitude degree coordinates.
   *
   * @return the latitude/longitude geometry
   * @throws GeometryException         if an unexpected but recoverable error occurs creating this geometry that should
   *                                   be logged for debugging
   * @throws GeometryException.Verbose if an expected error occurs creating this geometry that will be logged at a lower
   *                                   log level
   */
  public abstract Geometry latLonGeometry() throws GeometryException;

  /**
   * Returns this feature's geometry in world web mercator coordinates.
   *
   * @return the geometry in web mercator coordinates
   * @throws GeometryException         if an unexpected but recoverable error occurs creating this geometry that should
   *                                   be logged for debugging
   * @throws GeometryException.Verbose if an expected error occurs creating this geometry that will be logged at a lower
   *                                   log level
   */
  public abstract Geometry worldGeometry() throws GeometryException;

  /** Returns and caches {@link Geometry#getCentroid()} of this geometry in world web mercator coordinates. */
  public final Geometry centroid() throws GeometryException {
    return centroid != null ? centroid : (centroid =
      canBePolygon() ? polygon().getCentroid() :
        canBeLine() ? line().getCentroid() :
        worldGeometry().getCentroid());
  }

  /** Returns and caches {@link Geometry#getInteriorPoint()} of this geometry in world web mercator coordinates. */
  public final Geometry pointOnSurface() throws GeometryException {
    return pointOnSurface != null ? pointOnSurface : (pointOnSurface =
      canBePolygon() ? polygon().getInteriorPoint() :
        canBeLine() ? line().getInteriorPoint() :
        worldGeometry().getInteriorPoint());
  }

  private Geometry computeCentroidIfConvex() throws GeometryException {
    if (!canBePolygon()) {
      return centroid();
    } else if (polygon()instanceof Polygon poly &&
      poly.getNumInteriorRing() == 0 &&
      GeoUtils.isConvex(poly.getExteriorRing())) {
      return centroid();
    } else { // multipolygon, polygon with holes, or concave polygon
      return pointOnSurface();
    }
  }

  /**
   * Returns and caches a point inside the geometry in world web mercator coordinates.
   * <p>
   * If the geometry is convex, uses the faster {@link Geometry#getCentroid()} but otherwise falls back to the slower
   * {@link Geometry#getInteriorPoint()}.
   */
  public final Geometry centroidIfConvex() throws GeometryException {
    return centroidIfConvex != null ? centroidIfConvex : (centroidIfConvex = computeCentroidIfConvex());
  }

  /**
   * Computes this feature as a {@link LineString} or {@link MultiLineString} in world web mercator coordinates.
   *
   * @return the linestring in web mercator coordinates
   * @throws GeometryException         if an unexpected but recoverable error occurs creating this geometry that should
   *                                   be logged for debugging
   * @throws GeometryException.Verbose if an expected error occurs creating this geometry that will be logged at a lower
   *                                   log level
   */
  protected Geometry computeLine() throws GeometryException {
    Geometry world = worldGeometry();
    return world instanceof Lineal ? world : GeoUtils.polygonToLineString(world);
  }

  /**
   * Returns this feature as a {@link LineString} or {@link MultiLineString} in world web mercator coordinates.
   *
   * @throws GeometryException if an error occurs constructing the geometry, or of this feature should not be
   *                           interpreted as a line
   */
  public final Geometry line() throws GeometryException {
    if (!canBeLine()) {
      throw new GeometryException("feature_not_line", "cannot be line");
    }
    if (linearGeometry == null) {
      linearGeometry = computeLine();
    }
    return linearGeometry;
  }

  /**
   * Computes this feature as a {@link Polygon} or {@link MultiPolygon} in world web mercator coordinates.
   *
   * @return the polygon in web mercator coordinates
   * @throws GeometryException         if an unexpected but recoverable error occurs creating this geometry that should
   *                                   be logged for debugging
   * @throws GeometryException.Verbose if an expected error occurs creating this geometry that will be logged at a lower
   *                                   log level
   */
  protected Geometry computePolygon() throws GeometryException {
    return worldGeometry();
  }

  /**
   * Returns this feature as a {@link Polygon} or {@link MultiPolygon} in world web mercator coordinates.
   *
   * @throws GeometryException if an error occurs constructing the geometry, or of this feature should not be
   *                           interpreted as a line
   */
  public final Geometry polygon() throws GeometryException {
    if (!canBePolygon()) {
      throw new GeometryException("feature_not_polygon", "cannot be polygon");
    }
    return polygonGeometry != null ? polygonGeometry : (polygonGeometry = computePolygon());
  }

  private Geometry computeValidPolygon() throws GeometryException {
    Geometry polygon = polygon();
    if (!polygon.isValid()) {
      polygon = GeoUtils.fixPolygon(polygon);
    }
    return polygon;
  }

  /**
   * Returns this feature as a valid {@link Polygon} or {@link MultiPolygon} in world web mercator coordinates.
   * <p>
   * Validating and fixing invalid polygons can be expensive, so use only if necessary. Invalid polygons will also be
   * fixed at render-time.
   *
   * @throws GeometryException if an error occurs constructing the geometry, or of this feature should not be
   *                           interpreted as a line
   */
  public final Geometry validatedPolygon() throws GeometryException {
    if (!canBePolygon()) {
      throw new GeometryException("feature_not_polygon", "cannot be polygon");
    }
    return validPolygon != null ? validPolygon : (validPolygon = computeValidPolygon());
  }

  /**
   * Returns and caches the result of {@link Geometry#getArea()} of this feature in world web mercator coordinates where
   * {@code 1} means the area of the entire planet.
   */
  public double area() throws GeometryException {
    return Double.isNaN(area) ? (area = canBePolygon() ? polygon().getArea() : 0) : area;
  }

  /**
   * Returns and caches the result of {@link Geometry#getLength()} of this feature in world web mercator coordinates
   * where {@code 1} means the circumference of the entire planet or the distance from 85 degrees north to 85 degrees
   * south.
   */
  public double length() throws GeometryException {
    return Double.isNaN(length) ? (length =
      (isPoint() || canBePolygon() || canBeLine()) ? worldGeometry().getLength() : 0) : length;
  }

  /** Returns true if this feature can be interpreted as a {@link Point} or {@link MultiPoint}. */
  public abstract boolean isPoint();

  /**
   * Returns true if this feature can be interpreted as a {@link Polygon} or {@link MultiPolygon}.
   * <p>
   * A closed ring can either be a polygon or linestring, so return false to not allow this closed ring to be treated as
   * a polygon.
   */
  public abstract boolean canBePolygon();

  /**
   * Returns true if this feature can be interpreted as a {@link LineString} or {@link MultiLineString}.
   * <p>
   * A closed ring can either be a polygon or linestring, so return false to not allow this closed ring to be treated as
   * a linestring.
   */
  public abstract boolean canBeLine();

  /** Returns the ID of the source that this feature came from. */
  public String getSource() {
    return source;
  }

  /** Returns the layer ID within a source that this feature comes from. */
  public String getSourceLayer() {
    return sourceLayer;
  }


  /**
   * Returns a list of OSM relations that this element belongs to.
   *
   * @param relationInfoClass class of the processed relation data
   * @param <T>               type of {@code relationInfoClass}
   * @return A list containing the OSM relation info along with the role that this element is tagged with in that
   *         relation
   */
  // TODO this should be in a specialized OSM subclass, not the generic superclass
  public <T extends OsmRelationInfo> List<OsmReader.RelationMember<T>> relationInfo(
    Class<T> relationInfoClass) {
    List<OsmReader.RelationMember<T>> result = null;
    if (relationInfos != null) {
      for (OsmReader.RelationMember<?> info : relationInfos) {
        if (relationInfoClass.isInstance(info.relation())) {
          if (result == null) {
            result = new ArrayList<>();
          }
          @SuppressWarnings("unchecked") OsmReader.RelationMember<T> casted = (OsmReader.RelationMember<T>) info;
          result.add(casted);
        }
      }
    }
    return result == null ? List.of() : result;
  }

  /** Returns the ID for this element from the input data source (i.e. OSM element ID). */
  public final long id() {
    return id;
  }

  /** Returns true if this element has any OSM relation info. */
  public boolean hasRelationInfo() {
    return relationInfos != null && !relationInfos.isEmpty();
  }
}
