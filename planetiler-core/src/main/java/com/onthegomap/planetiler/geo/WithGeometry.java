package com.onthegomap.planetiler.geo;

import com.onthegomap.planetiler.reader.WithGeometryType;
import org.locationtech.jts.algorithm.construct.MaximumInscribedCircle;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.Lineal;
import org.locationtech.jts.geom.MultiLineString;
import org.locationtech.jts.geom.MultiPolygon;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.geom.Polygonal;
import org.locationtech.jts.geom.Puntal;

/**
 * Wraps a geometry and provides cached accessor methods for applying transformations and transforming to lat/lon.
 * <p>
 * All geometries except for {@link #latLonGeometry()} return elements in world web mercator coordinates where (0,0) is
 * the northwest corner and (1,1) is the southeast corner of the planet.
 */
public abstract class WithGeometry implements WithGeometryType {
  private Geometry centroid = null;
  private Geometry pointOnSurface = null;
  private Geometry centroidIfConvex = null;
  private double innermostPointTolerance = Double.NaN;
  private Geometry innermostPoint = null;
  private Geometry linearGeometry = null;
  private Geometry polygonGeometry = null;
  private Geometry validPolygon = null;
  private double area = Double.NaN;
  private double length = Double.NaN;
  private double areaMeters = Double.NaN;
  private double lengthMeters = Double.NaN;
  private LineSplitter lineSplitter;


  /**
   * Returns a geometry in world web mercator coordinates.
   *
   * @return the geometry in web mercator coordinates
   * @throws GeometryException         if an unexpected but recoverable error occurs creating this geometry that should
   *                                   be logged for debugging
   * @throws GeometryException.Verbose if an expected error occurs creating this geometry that will be logged at a lower
   *                                   log level
   */
  public abstract Geometry worldGeometry() throws GeometryException;


  /**
   * Returns this geometry in latitude/longitude degree coordinates.
   *
   * @return the latitude/longitude geometry
   * @throws GeometryException         if an unexpected but recoverable error occurs creating this geometry that should
   *                                   be logged for debugging
   * @throws GeometryException.Verbose if an expected error occurs creating this geometry that will be logged at a lower
   *                                   log level
   */
  public abstract Geometry latLonGeometry() throws GeometryException;


  /**
   * Returns and caches the result of {@link Geometry#getArea()} of this feature in world web mercator coordinates where
   * {@code 1} means the area of the entire planet.
   */
  public double area() throws GeometryException {
    return Double.isNaN(area) ? (area = canBePolygon() ? Math.abs(polygon().getArea()) : 0) : area;
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

  /**
   * Returns the sqrt of {@link #area()} if polygon or {@link #length()} if a line string.
   */
  public double size() throws GeometryException {
    return canBePolygon() ? Math.sqrt(Math.abs(area())) : canBeLine() ? length() : 0;
  }

  /** Returns the approximate area of the geometry in square meters. */
  public double areaMeters() throws GeometryException {
    return Double.isNaN(areaMeters) ? (areaMeters =
      (isPoint() || canBePolygon() || canBeLine()) ? GeoUtils.areaInMeters(latLonGeometry()) : 0) : areaMeters;
  }

  /** Returns the approximate length of the geometry in meters. */
  public double lengthMeters() throws GeometryException {
    return Double.isNaN(lengthMeters) ? (lengthMeters =
      (isPoint() || canBePolygon() || canBeLine()) ? GeoUtils.lengthInMeters(latLonGeometry()) : 0) : lengthMeters;
  }

  /** Returns the sqrt of {@link #areaMeters()} if polygon or {@link #lengthMeters()} if a line string. */
  public double sizeMeters() throws GeometryException {
    return canBePolygon() ? Math.sqrt(Math.abs(areaMeters())) : canBeLine() ? lengthMeters() : 0;
  }


  /** Returns the length of this geometry in units of {@link Unit.Length}. */
  public double length(Unit.Length length) {
    return length.of(this);
  }

  /**
   * Returns the length of this geometry if it is a line or the square root of the area if it is a polygon in units of
   * {@link Unit.Length}.
   */
  public double size(Unit.Length length) {
    return canBePolygon() ? Math.sqrt(length.base().area(this)) : length.base().length(this);
  }

  /** Returns the area of this geometry in units of {@link Unit.Area}. */
  public double area(Unit.Area area) {
    return area.of(this);
  }

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

  /**
   * Returns {@link MaximumInscribedCircle#getCenter()} of this geometry in world web mercator coordinates.
   *
   * @param tolerance precision for calculating maximum inscribed circle. 0.01 means 1% of the square root of the area.
   *                  Smaller values for a more precise tolerance become very expensive to compute. Values between
   *                  0.05-0.1 are a good compromise of performance vs. precision.
   */
  public final Geometry innermostPoint(double tolerance) throws GeometryException {
    if (canBePolygon()) {
      // cache as long as the tolerance hasn't changed
      if (tolerance != innermostPointTolerance || innermostPoint == null) {
        innermostPoint = MaximumInscribedCircle.getCenter(polygon(), Math.sqrt(area()) * tolerance);
        innermostPointTolerance = tolerance;
      }
      return innermostPoint;
    } else if (canBeLine()) {
      return lineMidpoint();
    } else {
      return pointOnSurface();
    }
  }

  /**
   * Returns the midpoint of this line, or the longest segment if it is a multilinestring.
   */
  public final Geometry lineMidpoint() throws GeometryException {
    if (innermostPoint == null) {
      innermostPoint = pointAlongLine(0.5);
    }
    return innermostPoint;
  }

  /**
   * Returns along this line where {@code ratio=0} is the start {@code ratio=1} is the end and {@code ratio=0.5} is the
   * midpoint.
   * <p>
   * When this is a multilinestring, the longest segment is used.
   */
  public final Geometry pointAlongLine(double ratio) throws GeometryException {
    if (lineSplitter == null) {
      var line = line();
      lineSplitter = new LineSplitter(line instanceof MultiLineString multi ? GeoUtils.getLongestLine(multi) : line);
    }
    return lineSplitter.get(ratio);
  }

  private Geometry computeCentroidIfConvex() throws GeometryException {
    if (!canBePolygon()) {
      return centroid();
    } else if (polygon() instanceof Polygon poly &&
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
      throw new GeometryException("feature_not_line", "cannot be line", true);
    }
    if (linearGeometry == null) {
      linearGeometry = computeLine();
    }
    return linearGeometry;
  }

  /**
   * Returns a partial line string from {@code start} to {@code end} where 0 is the beginning of the line and 1 is the
   * end of the line.
   *
   * @throws GeometryException if an error occurs constructing the geometry, or of this feature should not be
   *                           interpreted as a single line (multilinestrings are not allowed).
   */
  public final Geometry partialLine(double start, double end) throws GeometryException {
    Geometry line = line();
    if (start <= 0 && end >= 1) {
      return line;
    } else if (line instanceof LineString lineString) {
      if (this.lineSplitter == null) {
        this.lineSplitter = new LineSplitter(lineString);
      }
      return lineSplitter.get(start, end);
    } else {
      throw new GeometryException("partial_multilinestring", "cannot get partial of a multiline", true);
    }
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
      throw new GeometryException("feature_not_polygon", "cannot be polygon", true);
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
      throw new GeometryException("feature_not_polygon", "cannot be polygon", true);
    }
    return validPolygon != null ? validPolygon : (validPolygon = computeValidPolygon());
  }

  /** Wraps a world web mercator geometry. */
  public static WithGeometry fromWorldGeometry(Geometry worldGeometry) {
    return new FromWorld(worldGeometry);
  }

  private static class FromWorld extends WithGeometry {
    private final Geometry worldGeometry;
    private Geometry latLonGeometry;

    FromWorld(Geometry worldGeometry) {
      this.worldGeometry = worldGeometry;
    }

    @Override
    public Geometry worldGeometry() {
      return worldGeometry;
    }

    @Override
    public Geometry latLonGeometry() {
      return latLonGeometry != null ? latLonGeometry : (latLonGeometry = GeoUtils.worldToLatLonCoords(worldGeometry));
    }

    @Override
    public boolean isPoint() {
      return worldGeometry instanceof Puntal;
    }

    @Override
    public boolean canBePolygon() {
      return worldGeometry instanceof Polygonal;
    }

    @Override
    public boolean canBeLine() {
      return worldGeometry instanceof Lineal;
    }

    @Override
    public boolean equals(Object obj) {
      return obj == this || (obj instanceof FromWorld other && other.worldGeometry.equals(worldGeometry));
    }

    @Override
    public int hashCode() {
      return worldGeometry.hashCode();
    }
  }
}
