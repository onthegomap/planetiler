package com.onthegomap.planetiler.geo;

import clipper2.Clipper;
import clipper2.core.FillRule;
import clipper2.core.Path64;
import clipper2.core.Paths64;
import com.onthegomap.planetiler.collection.LongLongMap;
import com.onthegomap.planetiler.reader.osm.OsmMultipolygon;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
import org.locationtech.jts.algorithm.Area;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.CoordinateSequence;
import org.locationtech.jts.geom.CoordinateXY;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryCollection;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.LineSegment;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.LinearRing;
import org.locationtech.jts.geom.MultiPolygon;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.geom.PrecisionModel;
import org.locationtech.jts.geom.TopologyException;
import org.locationtech.jts.geom.impl.PackedCoordinateSequence;
import org.locationtech.jts.geom.impl.PackedCoordinateSequenceFactory;
import org.locationtech.jts.geom.util.GeometryTransformer;
import org.locationtech.jts.io.WKBReader;
import org.locationtech.jts.precision.GeometryPrecisionReducer;

/**
 * A collection of utilities for working with JTS data structures and geographic data.
 * <p>
 * "world" coordinates in this class refer to web mercator coordinates where the top-left/northwest corner of the map is
 * (0,0) and bottom-right/southeast corner is (1,1).
 */
public class GeoUtils {

  /** Rounding precision for 256x256px tiles encoded using 4096 values. */
  public static final PrecisionModel TILE_PRECISION = new PrecisionModel(4096d / 256d);
  public static final GeometryFactory JTS_FACTORY = new GeometryFactory(PackedCoordinateSequenceFactory.DOUBLE_FACTORY);
  public static final WKBReader WKB_READER = new WKBReader(JTS_FACTORY);
  public static final Geometry EMPTY_GEOMETRY = JTS_FACTORY.createGeometryCollection();
  public static final Point EMPTY_POINT = JTS_FACTORY.createPoint();
  public static final LineString EMPTY_LINE = JTS_FACTORY.createLineString();
  public static final Polygon EMPTY_POLYGON = JTS_FACTORY.createPolygon();
  private static final LineString[] EMPTY_LINE_STRING_ARRAY = new LineString[0];
  private static final Polygon[] EMPTY_POLYGON_ARRAY = new Polygon[0];
  private static final Point[] EMPTY_POINT_ARRAY = new Point[0];
  private static final double WORLD_RADIUS_METERS = 6_378_137;
  public static final double WORLD_CIRCUMFERENCE_METERS = Math.PI * 2 * WORLD_RADIUS_METERS;
  private static final double RADIANS_PER_DEGREE = Math.PI / 180;
  private static final double DEGREES_PER_RADIAN = 180 / Math.PI;
  /**
   * Transform web mercator coordinates where top-left corner of the planet is (0,0) and bottom-right is (1,1) to
   * latitude/longitude coordinates.
   */
  private static final GeometryTransformer UNPROJECT_WORLD_COORDS = new GeometryTransformer() {
    @Override
    protected CoordinateSequence transformCoordinates(CoordinateSequence coords, Geometry parent) {
      CoordinateSequence copy = new PackedCoordinateSequence.Double(coords.size(), 2, 0);
      for (int i = 0; i < coords.size(); i++) {
        copy.setOrdinate(i, 0, getWorldLon(coords.getX(i)));
        copy.setOrdinate(i, 1, getWorldLat(coords.getY(i)));
      }
      return copy;
    }
  };
  /**
   * Transform latitude/longitude coordinates to web mercator where top-left corner of the planet is (0,0) and
   * bottom-right is (1,1).
   */
  private static final GeometryTransformer PROJECT_WORLD_COORDS = new GeometryTransformer() {
    @Override
    protected CoordinateSequence transformCoordinates(CoordinateSequence coords, Geometry parent) {
      CoordinateSequence copy = new PackedCoordinateSequence.Double(coords.size(), 2, 0);
      for (int i = 0; i < coords.size(); i++) {
        copy.setOrdinate(i, 0, getWorldX(coords.getX(i)));
        copy.setOrdinate(i, 1, getWorldY(coords.getY(i)));
      }
      return copy;
    }
  };
  private static final double MAX_LAT = getWorldLat(-0.1);
  private static final double MIN_LAT = getWorldLat(1.1);
  // to pack latitude/longitude into a single long, we round them to 31 bits of precision
  private static final double QUANTIZED_WORLD_SIZE = Math.pow(2, 31);
  private static final double HALF_QUANTIZED_WORLD_SIZE = QUANTIZED_WORLD_SIZE / 2;
  private static final long LOWER_32_BIT_MASK = (1L << 32) - 1L;
  /**
   * Bounds for the entire area of the planet that a web mercator projection covers, where top left is (0,0) and bottom
   * right is (1,1).
   */
  public static final Envelope WORLD_BOUNDS = new Envelope(0, 1, 0, 1);
  /**
   * Bounds for the entire area of the planet that a web mercator projection covers in latitude/longitude coordinates.
   */
  public static final Envelope WORLD_LAT_LON_BOUNDS = toLatLonBoundsBounds(WORLD_BOUNDS);

  // should not instantiate
  private GeoUtils() {}

  /**
   * Returns a copy of {@code geom} transformed from latitude/longitude coordinates to web mercator where top-left
   * corner of the planet is (0,0) and bottom-right is (1,1).
   */
  public static Geometry latLonToWorldCoords(Geometry geom) {
    return PROJECT_WORLD_COORDS.transform(geom);
  }

  /**
   * Returns a copy of {@code geom} transformed from web mercator where top-left corner of the planet is (0,0) and
   * bottom-right is (1,1) to latitude/longitude.
   */
  public static Geometry worldToLatLonCoords(Geometry geom) {
    return UNPROJECT_WORLD_COORDS.transform(geom);
  }

  /**
   * Returns a copy of {@code worldBounds} transformed from web mercator where top-left corner of the planet is (0,0)
   * and bottom-right is (1,1) to latitude/longitude.
   */
  public static Envelope toLatLonBoundsBounds(Envelope worldBounds) {
    return new Envelope(
      getWorldLon(worldBounds.getMinX()),
      getWorldLon(worldBounds.getMaxX()),
      getWorldLat(worldBounds.getMinY()),
      getWorldLat(worldBounds.getMaxY()));
  }

  /**
   * Returns a copy of {@code lonLatBounds} transformed from latitude/longitude coordinates to web mercator where
   * top-left corner of the planet is (0,0) and bottom-right is (1,1).
   */
  public static Envelope toWorldBounds(Envelope lonLatBounds) {
    return new Envelope(
      getWorldX(lonLatBounds.getMinX()),
      getWorldX(lonLatBounds.getMaxX()),
      getWorldY(lonLatBounds.getMinY()),
      getWorldY(lonLatBounds.getMaxY())
    );
  }

  /**
   * Returns the longitude for a web mercator coordinate {@code x} where 0 is the international date line on the west
   * side, 1 is the international date line on the east side, and 0.5 is the prime meridian.
   */
  public static double getWorldLon(double x) {
    return x * 360 - 180;
  }

  /**
   * Returns the latitude for a web mercator {@code y} coordinate where 0 is the north edge of the map, 0.5 is the
   * equator, and 1 is the south edge of the map.
   */
  public static double getWorldLat(double y) {
    double n = Math.PI - 2 * Math.PI * y;
    return DEGREES_PER_RADIAN * Math.atan(0.5 * (Math.exp(n) - Math.exp(-n)));
  }

  /**
   * Returns the web mercator X coordinate for {@code longitude} where 0 is the international date line on the west
   * side, 1 is the international date line on the east side, and 0.5 is the prime meridian.
   */
  public static double getWorldX(double longitude) {
    return (longitude + 180) / 360;
  }

  /**
   * Returns the web mercator Y coordinate for {@code latitude} where 0 is the north edge of the map, 0.5 is the
   * equator, and 1 is the south edge of the map.
   */
  public static double getWorldY(double latitude) {
    if (latitude <= MIN_LAT) {
      return 1.1;
    }
    if (latitude >= MAX_LAT) {
      return -0.1;
    }
    double sin = Math.sin(latitude * RADIANS_PER_DEGREE);
    return 0.5 - 0.25 * Math.log((1 + sin) / (1 - sin)) / Math.PI;
  }

  /**
   * Returns a latitude/longitude coordinate encoded into a single 64-bit long for storage in a {@link LongLongMap} that
   * can be decoded using {@link #decodeWorldX(long)} and {@link #decodeWorldY(long)}.
   */
  public static long encodeFlatLocation(double lon, double lat) {
    double worldX = getWorldX(lon) + 1;
    double worldY = getWorldY(lat) + 1;
    long x = (long) (worldX * HALF_QUANTIZED_WORLD_SIZE);
    long y = (long) (worldY * HALF_QUANTIZED_WORLD_SIZE);
    return (x << 32) | (y & LOWER_32_BIT_MASK);
  }

  /**
   * Returns the web mercator Y coordinate of the latitude/longitude encoded with
   * {@link #encodeFlatLocation(double, double)}.
   */
  public static double decodeWorldY(long encoded) {
    return ((encoded & LOWER_32_BIT_MASK) / HALF_QUANTIZED_WORLD_SIZE) - 1;
  }

  /**
   * Returns the web mercator X coordinate of the latitude/longitude encoded with
   * {@link #encodeFlatLocation(double, double)}.
   */
  public static double decodeWorldX(long encoded) {
    return ((encoded >>> 32) / HALF_QUANTIZED_WORLD_SIZE) - 1;
  }

  /**
   * Returns an approximate zoom level that a map should be displayed at to show all of {@code envelope}, specified in
   * latitude/longitude coordinates.
   */
  public static double getZoomFromLonLatBounds(Envelope envelope) {
    Envelope worldBounds = GeoUtils.toWorldBounds(envelope);
    return getZoomFromWorldBounds(worldBounds);
  }

  /**
   * Returns an approximate zoom level that a map should be displayed at to show all of {@code envelope}, specified in
   * web mercator coordinates.
   */
  public static double getZoomFromWorldBounds(Envelope worldBounds) {
    double maxEdge = Math.max(worldBounds.getWidth(), worldBounds.getHeight());
    return Math.max(0, -Math.log(maxEdge) / Math.log(2));
  }

  /** Returns the width in meters of a single pixel of a 256x256 px tile at the given {@code zoom} level. */
  public static double metersPerPixelAtEquator(int zoom) {
    return WORLD_CIRCUMFERENCE_METERS / Math.pow(2d, zoom + 8d);
  }

  /** Returns the length in pixels for a given number of meters on a 256x256 px tile at the given {@code zoom} level. */
  public static double metersToPixelAtEquator(int zoom, double meters) {
    return meters / metersPerPixelAtEquator(zoom);
  }

  public static Point point(double x, double y) {
    return JTS_FACTORY.createPoint(new CoordinateXY(x, y));
  }

  public static Point point(Coordinate coord) {
    return JTS_FACTORY.createPoint(coord);
  }

  public static Geometry createMultiLineString(List<LineString> lineStrings) {
    return JTS_FACTORY.createMultiLineString(lineStrings.toArray(EMPTY_LINE_STRING_ARRAY));
  }

  public static Geometry createMultiPolygon(List<Polygon> polygon) {
    return JTS_FACTORY.createMultiPolygon(polygon.toArray(EMPTY_POLYGON_ARRAY));
  }

  /**
   * Attempt to fix any self-intersections or overlaps in {@code geom}.
   *
   * @throws GeometryException if a robustness error occurred
   */
  public static Geometry fixPolygon(Geometry geom) throws GeometryException {
    try {
      return geom.buffer(0);
    } catch (TopologyException e) {
      throw new GeometryException("fix_polygon_topology_error", "robustness error fixing polygon: " + e);
    }
  }

  public static Geometry combineLineStrings(List<LineString> lineStrings) {
    return lineStrings.size() == 1 ? lineStrings.get(0) : createMultiLineString(lineStrings);
  }

  public static Geometry combinePolygons(List<Polygon> polys) {
    return polys.size() == 1 ? polys.get(0) : createMultiPolygon(polys);
  }

  public static Geometry combinePoints(List<Point> points) {
    return points.size() == 1 ? points.get(0) : createMultiPoint(points);
  }

  public static Path64 toClipper2(CoordinateSequence seq) {
    int[] result = new int[seq.size() * 2];
    for (int i = 0; i < seq.size(); i++) {
      result[i * 2] = (int) Math.round(seq.getX(i) * 4096d / 256d);
      result[i * 2 + 1] = (int) Math.round(seq.getY(i) * 4096d / 256d);
    }
    return Clipper.MakePath(result);
  }

  public static void toClipper2(Geometry geom, Paths64 result) {
    if (geom instanceof Polygon p) {
      result.add(toClipper2(p.getExteriorRing().getCoordinateSequence()));
      for (int i = 0; i < p.getNumInteriorRing(); i++) {
        result.add(toClipper2(p.getInteriorRingN(i).getCoordinateSequence()));
      }
    } else if (geom instanceof MultiPolygon p) {
      for (int i = 0; i < p.getNumGeometries(); i++) {
        toClipper2(p.getGeometryN(i), result);
      }
    } else {
      throw new IllegalArgumentException("Unhandled " + geom.getGeometryType());
    }
  }

  public static Paths64 toClipper2(Geometry geom) {
    var result = new Paths64();
    toClipper2(geom, result);
    return result;
  }

  public static Geometry fromClipper2(Paths64 geom) {
    if (geom.isEmpty())
      return EMPTY_GEOMETRY;
    List<CoordinateSequence> seqs = new ArrayList<>(geom.size());
    int j = 0;
    for (var path : geom) {
      double[] result = new double[path.size() * 2 + 2];
      for (int i = 0; i < path.size(); i++) {
        var point = path.get(i);
        result[i * 2] = point.x * 256d / 4096d;
        result[i * 2 + 1] = point.y * 256d / 4096d;
      }
      result[result.length - 2] = result[0];
      result[result.length - 1] = result[1];
      seqs.add(new PackedCoordinateSequence.Double(result, 2, 0));
    }
    try {
      return OsmMultipolygon.build(seqs);
    } catch (GeometryException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Returns a copy of {@code geom} with coordinates rounded to {@link #TILE_PRECISION} and fixes any polygon
   * self-intersections or overlaps that may have caused.
   */
  public static Geometry snapAndFixPolygon(Geometry geom) throws GeometryException {
    var clipper = toClipper2(geom);
    var result = Clipper.Union(clipper, FillRule.NonZero);
    return fromClipper2(result);
  }

  /**
   * Returns a copy of {@code geom} with coordinates rounded to {@code #tilePrecision} and fixes any polygon
   * self-intersections or overlaps that may have caused.
   *
   * @throws GeometryException if an unrecoverable robustness exception prevents us from fixing the geometry
   */
  public static Geometry snapAndFixPolygon(Geometry geom, PrecisionModel tilePrecision) throws GeometryException {
    try {
      return GeometryPrecisionReducer.reduce(geom, tilePrecision);
    } catch (IllegalArgumentException e) {
      // precision reduction fails if geometry is invalid, so attempt
      // to fix it then try again
      geom = fixPolygon(geom);
      try {
        return GeometryPrecisionReducer.reduce(geom, tilePrecision);
      } catch (IllegalArgumentException e2) {
        // give it one last try, just in case
        geom = fixPolygon(geom);
        try {
          return GeometryPrecisionReducer.reduce(geom, tilePrecision);
        } catch (IllegalArgumentException e3) {
          throw new GeometryException("snap_third_time_failed", "Error reducing precision");
        }
      }
    }
  }

  private static double wrapDouble(double value, double max) {
    value %= max;
    if (value < 0) {
      value += max;
    }
    return value;
  }


  private static long longPair(int a, int b) {
    return (((long) a) << 32L) | (b & LOWER_32_BIT_MASK);
  }

  /**
   * Breaks the world up into a grid and returns an ID for the square that {@code coord} falls into.
   *
   * @param tilesAtZoom       the tile width of the world at this zoom level
   * @param labelGridTileSize the tile width of each grid square
   * @param coord             the coordinate, scaled to this zoom level
   * @return an ID representing the grid square that {@code coord} falls into.
   */
  public static long labelGridId(int tilesAtZoom, double labelGridTileSize, Coordinate coord) {
    return GeoUtils.longPair(
      (int) Math.floor(wrapDouble(coord.getX(), tilesAtZoom) / labelGridTileSize),
      (int) Math.floor((coord.getY()) / labelGridTileSize)
    );
  }

  /** Returns a {@link CoordinateSequence} from a list of {@code x, y, x, y, ...} coordinates. */
  public static CoordinateSequence coordinateSequence(double... coords) {
    return new PackedCoordinateSequence.Double(coords, 2, 0);
  }

  public static Geometry createMultiPoint(List<Point> points) {
    return JTS_FACTORY.createMultiPoint(points.toArray(EMPTY_POINT_ARRAY));
  }

  /**
   * Returns line strings for every inner and outer ring contained in a polygon.
   *
   * @throws GeometryException if {@code geom} contains anything other than polygons or line strings
   */
  public static Geometry polygonToLineString(Geometry geom) throws GeometryException {
    List<LineString> lineStrings = new ArrayList<>();
    getLineStrings(geom, lineStrings);
    if (lineStrings.isEmpty()) {
      throw new GeometryException("polygon_to_linestring_empty", "No line strings");
    } else if (lineStrings.size() == 1) {
      return lineStrings.get(0);
    } else {
      return createMultiLineString(lineStrings);
    }
  }

  private static void getLineStrings(Geometry input, List<LineString> output) throws GeometryException {
    if (input instanceof LinearRing linearRing) {
      output.add(JTS_FACTORY.createLineString(linearRing.getCoordinateSequence()));
    } else if (input instanceof LineString lineString) {
      output.add(lineString);
    } else if (input instanceof Polygon polygon) {
      getLineStrings(polygon.getExteriorRing(), output);
      for (int i = 0; i < polygon.getNumInteriorRing(); i++) {
        getLineStrings(polygon.getInteriorRingN(i), output);
      }
    } else if (input instanceof GeometryCollection gc) {
      for (int i = 0; i < gc.getNumGeometries(); i++) {
        getLineStrings(gc.getGeometryN(i), output);
      }
    } else {
      throw new GeometryException("get_line_strings_bad_type",
        "unrecognized geometry type: " + input.getGeometryType());
    }
  }

  public static Geometry createGeometryCollection(List<Geometry> polygonGroup) {
    return JTS_FACTORY.createGeometryCollection(polygonGroup.toArray(Geometry[]::new));
  }

  /** Returns a point approximately {@code ratio} of the way from start to end and {@code offset} units to the right. */
  public static Point pointAlongOffset(LineString lineString, double ratio, double offset) {
    int numPoints = lineString.getNumPoints();
    int middle = Math.max(0, Math.min(numPoints - 2, (int) (numPoints * ratio)));
    Coordinate a = lineString.getCoordinateN(middle);
    Coordinate b = lineString.getCoordinateN(middle + 1);
    LineSegment segment = new LineSegment(a, b);
    return JTS_FACTORY.createPoint(segment.pointAlongOffset(0.5, offset));
  }

  public static Polygon createPolygon(LinearRing exteriorRing, List<LinearRing> rings) {
    return JTS_FACTORY.createPolygon(exteriorRing, rings.toArray(LinearRing[]::new));
  }


  /**
   * Returns {@code false} if the signed area of the triangle formed by 3 sequential points changes sign anywhere along
   * {@code ring}, ignoring repeated and collinear points.
   */
  public static boolean isConvex(LinearRing ring) {
    return !isConcave(ring);
  }

  /**
   * Returns {@code true} if the signed area of the triangle formed by 3 sequential points changes sign anywhere along
   * {@code ring}, ignoring repeated and collinear points.
   */
  public static boolean isConcave(LinearRing ring) {
    CoordinateSequence seq = ring.getCoordinateSequence();
    if (seq.size() <= 3) {
      return false;
    }

    // ignore leading repeated points
    double c0x = seq.getX(0);
    double c0y = seq.getY(0);
    double c1x = Double.NaN, c1y = Double.NaN;
    int i;
    for (i = 1; i < seq.size(); i++) {
      c1x = seq.getX(i);
      c1y = seq.getY(i);
      if (c1x != c0x || c1y != c0y) {
        break;
      }
    }

    double dx1 = c1x - c0x;
    double dy1 = c1y - c0y;

    int sign = 0;

    for (; i < seq.size(); i++) {
      double c2x = seq.getX(i);
      double c2y = seq.getY(i);

      double dx2 = c2x - c1x;
      double dy2 = c2y - c1y;
      double z = dx1 * dy2 - dy1 * dx2;

      // if z == 0 (with small delta to account for rounding errors) then keep skipping
      // points to ignore repeated or collinear points
      if (Math.abs(z) < 1e-10) {
        continue;
      }

      int s = z >= 0d ? 1 : -1;
      if (sign == 0) {
        // on the first non-repeated, non-collinear points, store sign of the area for comparison
        sign = s;
      } else if (sign != s) {
        // the sign of this triangle has changed, not convex
        return false;
      }

      c1x = c2x;
      c1y = c2y;
      dx1 = dx2;
      dy1 = dy2;
    }
    return true;
  }

  /**
   * If {@code geometry} is a {@link MultiPolygon}, returns a copy with polygons sorted by descending area of the outer
   * shell, otherwise returns the input geometry.
   */
  public static Geometry sortPolygonsByAreaDescending(Geometry geometry) {
    if (geometry instanceof MultiPolygon multiPolygon) {
      PolyAndArea[] areas = new PolyAndArea[multiPolygon.getNumGeometries()];
      for (int i = 0; i < multiPolygon.getNumGeometries(); i++) {
        areas[i] = new PolyAndArea((Polygon) multiPolygon.getGeometryN(i));
      }
      return JTS_FACTORY.createMultiPolygon(
        Stream.of(areas).sorted().map(d -> d.poly).toArray(Polygon[]::new)
      );
    } else {
      return geometry;
    }
  }

  /** Combines multiple geometries into one {@link GeometryCollection}. */
  public static Geometry combine(Geometry... geometries) {
    List<Geometry> innerGeometries = new ArrayList<>();
    // attempt to flatten out nested geometry collections
    for (var geom : geometries) {
      if (geom instanceof GeometryCollection collection) {
        for (int i = 0; i < collection.getNumGeometries(); i++) {
          innerGeometries.add(collection.getGeometryN(i));
        }
      } else {
        innerGeometries.add(geom);
      }
    }
    return innerGeometries.size() == 1 ? innerGeometries.get(0) :
      JTS_FACTORY.createGeometryCollection(innerGeometries.toArray(Geometry[]::new));
  }

  /** Helper class to sort polygons by area of their outer shell. */
  private record PolyAndArea(Polygon poly, double area) implements Comparable<PolyAndArea> {

    PolyAndArea(Polygon poly) {
      this(poly, Area.ofRing(poly.getExteriorRing().getCoordinateSequence()));
    }

    @Override
    public int compareTo(PolyAndArea o) {
      return -Double.compare(area, o.area);
    }
  }
}
