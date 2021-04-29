package com.onthegomap.flatmap;

import com.onthegomap.flatmap.geo.GeoUtils;
import java.util.ArrayList;
import java.util.List;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.CoordinateSequence;
import org.locationtech.jts.geom.CoordinateXY;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryCollection;
import org.locationtech.jts.geom.MultiPoint;
import org.locationtech.jts.geom.MultiPolygon;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.geom.util.AffineTransformation;
import org.locationtech.jts.geom.util.GeometryTransformer;

public class TestUtils {

  public static final AffineTransformation TRANSFORM_TO_TILE = AffineTransformation
    .scaleInstance(256d / 4096d, 256d / 4096d);

  public static List<Coordinate> newCoordinateList(double... coords) {
    List<Coordinate> result = new ArrayList<>(coords.length / 2);
    for (int i = 0; i < coords.length; i += 2) {
      result.add(new CoordinateXY(coords[i], coords[i + 1]));
    }
    return result;
  }

  public static Polygon newPolygon(double... coords) {
    return GeoUtils.gf.createPolygon(newCoordinateList(coords).toArray(new Coordinate[0]));
  }

  public static Point newPoint(double x, double y) {
    return GeoUtils.gf.createPoint(new CoordinateXY(x, y));
  }

  public static Point newPointWithUserData(double x, double y, Object userData) {
    Point point = GeoUtils.gf.createPoint(new CoordinateXY(x, y));
    point.setUserData(userData);
    return point;
  }

  public static MultiPoint newMultiPoint(Point... points) {
    return GeoUtils.gf.createMultiPoint(points);
  }

  public static MultiPolygon newMultiPolygon(Polygon... polys) {
    return GeoUtils.gf.createMultiPolygon(polys);
  }

  public static GeometryCollection newGeometryCollection(Geometry... geoms) {
    return GeoUtils.gf.createGeometryCollection(geoms);
  }

  public static Geometry round(Geometry input) {
    return new GeometryTransformer() {
      @Override
      protected CoordinateSequence transformCoordinates(
        CoordinateSequence coords, Geometry parent) {
        for (int i = 0; i < coords.size(); i++) {
          for (int j = 0; j < coords.getDimension(); j++) {
            coords.setOrdinate(i, j, Math.round(coords.getOrdinate(i, j) * 1e5) / 1e5);
          }
        }
        return coords;
      }
    }.transform(input);
  }
}
