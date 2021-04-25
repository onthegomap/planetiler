package com.onthegomap.flatmap.geo;

import org.locationtech.jts.geom.CoordinateSequence;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.impl.PackedCoordinateSequence;
import org.locationtech.jts.geom.util.GeometryTransformer;
import org.locationtech.jts.io.WKBReader;

public class GeoUtils {

  public static final GeometryFactory gf = new GeometryFactory();
  public static final WKBReader wkbReader = new WKBReader(gf);

  private static final double DEGREES_TO_RADIANS = Math.PI / 180;
  private static final double RADIANS_TO_DEGREES = 180 / Math.PI;
  private static final double MAX_LAT = getWorldLat(-0.1);
  private static final double MIN_LAT = getWorldLat(1.1);
  public static double[] WORLD_BOUNDS = new double[]{0, 0, 1, 1};
  public static double[] WORLD_LAT_LON_BOUNDS = toLatLonBoundsBounds(WORLD_BOUNDS);

  public static double[] toLatLonBoundsBounds(double[] worldBounds) {
    return new double[]{
      getWorldLon(worldBounds[0]),
      getWorldLat(worldBounds[1]),
      getWorldLon(worldBounds[2]),
      getWorldLat(worldBounds[3])
    };
  }

  public static double[] toWorldBounds(double[] lonLatBounds) {
    return new double[]{
      getWorldX(lonLatBounds[0]),
      getWorldY(lonLatBounds[1]),
      getWorldX(lonLatBounds[2]),
      getWorldY(lonLatBounds[3])
    };
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

  public static final GeometryTransformer ProjectWorldCoords = new GeometryTransformer() {
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
}
