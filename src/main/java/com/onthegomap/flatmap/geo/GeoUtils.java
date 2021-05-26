package com.onthegomap.flatmap.geo;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Stream;
import org.jetbrains.annotations.NotNull;
import org.locationtech.jts.algorithm.Area;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.CoordinateSequence;
import org.locationtech.jts.geom.CoordinateXY;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryCollection;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.LinearRing;
import org.locationtech.jts.geom.MultiPoint;
import org.locationtech.jts.geom.MultiPolygon;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.geom.PrecisionModel;
import org.locationtech.jts.geom.impl.PackedCoordinateSequence;
import org.locationtech.jts.geom.util.GeometryTransformer;
import org.locationtech.jts.io.WKBReader;
import org.locationtech.jts.precision.GeometryPrecisionReducer;

public class GeoUtils {

  public static final GeometryFactory JTS_FACTORY = new GeometryFactory();
  public static final WKBReader wkbReader = new WKBReader(JTS_FACTORY);

  private static final LineString[] EMPTY_LINE_STRING_ARRAY = new LineString[0];
  private static final Coordinate[] EMPTY_COORD_ARRAY = new Coordinate[0];
  private static final Point[] EMPTY_POINT_ARRAY = new Point[0];

  private static final double WORLD_RADIUS_METERS = 6_378_137;
  private static final double WORLD_CIRCUMFERENCE_METERS = Math.PI * 2 * WORLD_RADIUS_METERS;
  private static final double DEGREES_TO_RADIANS = Math.PI / 180;
  private static final double RADIANS_TO_DEGREES = 180 / Math.PI;
  private static final double MAX_LAT = getWorldLat(-0.1);
  private static final double MIN_LAT = getWorldLat(1.1);
  public static Envelope WORLD_BOUNDS = new Envelope(0, 1, 0, 1);
  public static Envelope WORLD_LAT_LON_BOUNDS = toLatLonBoundsBounds(WORLD_BOUNDS);
  public static final GeometryTransformer UNPROJECT_WORLD_COORDS = new GeometryTransformer() {
    @Override
    protected CoordinateSequence transformCoordinates(CoordinateSequence coords, Geometry parent) {
      if (coords.getDimension() != 2) {
        throw new IllegalArgumentException("Dimension must be 2, was: " + coords.getDimension());
      }
      if (coords.getMeasures() != 0) {
        throw new IllegalArgumentException("Measures must be 0, was: " + coords.getMeasures());
      }
      CoordinateSequence copy = new PackedCoordinateSequence.Double(coords.size(), 2, 0);
      for (int i = 0; i < coords.size(); i++) {
        copy.setOrdinate(i, 0, getWorldLon(coords.getX(i)));
        copy.setOrdinate(i, 1, getWorldLat(coords.getY(i)));
      }
      return copy;
    }
  };
  public static final GeometryTransformer PROJECT_WORLD_COORDS = new GeometryTransformer() {
    @Override
    protected CoordinateSequence transformCoordinates(CoordinateSequence coords, Geometry parent) {
      if (coords.getDimension() != 2) {
        throw new IllegalArgumentException("Dimension must be 2, was: " + coords.getDimension());
      }
      if (coords.getMeasures() != 0) {
        throw new IllegalArgumentException("Measures must be 0, was: " + coords.getMeasures());
      }
      CoordinateSequence copy = new PackedCoordinateSequence.Double(coords.size(), 2, 0);
      for (int i = 0; i < coords.size(); i++) {
        copy.setOrdinate(i, 0, getWorldX(coords.getX(i)));
        copy.setOrdinate(i, 1, getWorldY(coords.getY(i)));
      }
      return copy;
    }
  };

  public static Geometry latLonToWorldCoords(Geometry geom) {
    return PROJECT_WORLD_COORDS.transform(geom);
  }

  public static Geometry worldToLatLonCoords(Geometry geom) {
    return UNPROJECT_WORLD_COORDS.transform(geom);
  }

  public static Envelope toLatLonBoundsBounds(Envelope worldBounds) {
    return new Envelope(
      getWorldLon(worldBounds.getMinX()),
      getWorldLon(worldBounds.getMaxX()),
      getWorldLat(worldBounds.getMinY()),
      getWorldLat(worldBounds.getMaxY()));
  }

  public static Envelope toWorldBounds(Envelope lonLatBounds) {
    return new Envelope(
      getWorldX(lonLatBounds.getMinX()),
      getWorldX(lonLatBounds.getMaxX()),
      getWorldY(lonLatBounds.getMinY()),
      getWorldY(lonLatBounds.getMaxY())
    );
  }

  public static double getWorldLon(double x) {
    return x * 360 - 180;
  }

  public static double getWorldLat(double y) {
    double n = Math.PI - 2 * Math.PI * y;
    return RADIANS_TO_DEGREES * Math.atan(0.5 * (Math.exp(n) - Math.exp(-n)));
  }

  public static double getWorldX(double lon) {
    return (lon + 180) / 360;
  }

  public static double getWorldY(double lat) {
    if (lat <= MIN_LAT) {
      return 1.1;
    }
    if (lat >= MAX_LAT) {
      return -0.1;
    }
    double sin = Math.sin(lat * DEGREES_TO_RADIANS);
    return 0.5 - 0.25 * Math.log((1 + sin) / (1 - sin)) / Math.PI;
  }

  private static final double QUANTIZED_WORLD_SIZE = Math.pow(2, 31);
  private static final long LOWER_32_BIT_MASK = (1L << 32) - 1L;

  public static long encodeFlatLocation(double lon, double lat) {
    double worldX = getWorldX(lon);
    double worldY = getWorldY(lat);
    long x = (long) (worldX * QUANTIZED_WORLD_SIZE);
    long y = (long) (worldY * QUANTIZED_WORLD_SIZE);
    return (x << 32) | y;
  }

  public static double decodeWorldY(long encoded) {
    return ((double) (encoded & LOWER_32_BIT_MASK)) / QUANTIZED_WORLD_SIZE;
  }

  public static double decodeWorldX(long encoded) {
    return ((double) (encoded >> 32)) / QUANTIZED_WORLD_SIZE;
  }

  public static double getZoomFromLonLatBounds(Envelope envelope) {
    Envelope worldBounds = GeoUtils.toWorldBounds(envelope);
    return getZoomFromWorldBounds(worldBounds);
  }

  public static double getZoomFromWorldBounds(Envelope worldBounds) {
    double maxEdge = Math.max(worldBounds.getWidth(), worldBounds.getHeight());
    return Math.max(0, -Math.log(maxEdge) / Math.log(2));
  }

  public static double metersPerPixelAtEquator(int zoom) {
    return WORLD_CIRCUMFERENCE_METERS / Math.pow(2, zoom + 8);
  }

  public static long longPair(int a, int b) {
    return (((long) a) << 32L) | (((long) b) & LOWER_32_BIT_MASK);
  }

  public static Point point(double x, double y) {
    return JTS_FACTORY.createPoint(new CoordinateXY(x, y));
  }

  public static Point point(Coordinate coord) {
    return JTS_FACTORY.createPoint(coord);
  }

  public static MultiPoint multiPoint(Collection<Coordinate> coords) {
    return JTS_FACTORY.createMultiPointFromCoords(coords.toArray(EMPTY_COORD_ARRAY));
  }

  public static Geometry createMultiLineString(List<LineString> lineStrings) {
    return JTS_FACTORY.createMultiLineString(lineStrings.toArray(EMPTY_LINE_STRING_ARRAY));
  }

  public static Geometry snapAndFixPolygon(Geometry geom, PrecisionModel tilePrecision) throws GeometryException {
    try {
      return GeometryPrecisionReducer.reduce(geom, tilePrecision);
    } catch (IllegalArgumentException e) {
      // precision reduction fails if geometry is invalid, so attempt
      // to fix it then try again
      geom = geom.buffer(0);
      try {
        return GeometryPrecisionReducer.reduce(geom, tilePrecision);
      } catch (IllegalArgumentException e2) {
        // give it one last try, just in case
        geom = geom.buffer(0);
        try {
          return GeometryPrecisionReducer.reduce(geom, tilePrecision);
        } catch (IllegalArgumentException e3) {
          throw new GeometryException("Error reducing precision");
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

  public static long labelGridId(int tilesAtZoom, double labelGridTileSize, Coordinate coord) {
    return GeoUtils.longPair(
      (int) Math.floor(wrapDouble(coord.getX() * tilesAtZoom, tilesAtZoom) / labelGridTileSize),
      (int) Math.floor((coord.getY() * tilesAtZoom) / labelGridTileSize)
    );
  }

  public static CoordinateSequence coordinateSequence(double... coords) {
    return new PackedCoordinateSequence.Double(coords, 2, 0);
  }

  public static Geometry createMultiPoint(List<Point> points) {
    return JTS_FACTORY.createMultiPoint(points.toArray(EMPTY_POINT_ARRAY));
  }

  public static Geometry polygonToLineString(Geometry world) throws GeometryException {
    List<LineString> lineStrings = new ArrayList<>();
    getLineStrings(world, lineStrings);
    if (lineStrings.size() == 0) {
      throw new GeometryException("No line strings");
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
      throw new GeometryException("unrecognized geometry type: " + input.getGeometryType());
    }
  }

  private static record PolyAndArea(Polygon poly, double area) implements Comparable<PolyAndArea> {

    PolyAndArea(Polygon poly) {
      this(poly, Area.ofRing(poly.getExteriorRing().getCoordinateSequence()));
    }

    @Override
    public int compareTo(@NotNull PolyAndArea o) {
      return -Double.compare(area, o.area);
    }
  }

  public static Geometry ensureDescendingPolygonsSizes(Geometry geometry) {
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
}
