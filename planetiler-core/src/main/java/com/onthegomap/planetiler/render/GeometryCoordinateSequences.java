package com.onthegomap.planetiler.render;

import com.onthegomap.planetiler.geo.GeoUtils;
import com.onthegomap.planetiler.geo.GeometryException;
import java.util.ArrayList;
import java.util.List;
import org.locationtech.jts.algorithm.Area;
import org.locationtech.jts.geom.CoordinateSequence;
import org.locationtech.jts.geom.CoordinateSequences;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryCollection;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.LinearRing;
import org.locationtech.jts.geom.MultiLineString;
import org.locationtech.jts.geom.MultiPolygon;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;

/**
 * Utility for converting back and forth between {@code Geometry} and {@code List<List<CoordinateSequence>>}
 * representing linestrings or polygons.
 * <p>
 * The {@code List<List<CoordinateSequence>>} format is:
 * <ul>
 * <li>For linestrings: {@code [[linestring], [linestring], ...]}</li> for each linestring in the collection
 * <li>For polygons: {@code [[outer ring, inner ring, inner ring], [outer ring, inner ring, ...], ...]}</li> for each
 * polygon in the multipolygon
 * </ul>
 */
class GeometryCoordinateSequences {

  /**
   * 从 {@code geom} 中提取每个线性组件的坐标序列，超过最小大小阈值。
   * <p>
   * 对于 {@link LineString LineStrings}，这意味着所有超过一定长度的线串。
   * <p>
   * 对于 {@link Polygon Polygons}，这意味着所有 [外环, 内环...] 环坐标序列列表，其中环超过一定面积。
   * 该实用工具还确保外环和内环使用逆时针方向。
   *
   * @param geom    一个或多个线串或多边形
   * @param minSize 线串的最小长度，或包含的外环/内环的最小面积
   * @return 几何体的坐标序列
   * @throws IllegalArgumentException 如果 {@code geom} 包含线串或多边形以外的内容（例如点）
   */
  static List<List<CoordinateSequence>> extractGroups(Geometry geom, double minSize) {
    List<List<CoordinateSequence>> result = new ArrayList<>();
    extractGroups(geom, result, minSize);
    return result;
  }

  /** 累积超过 {@code minSize} 的线性组件到 {@code groups} 中。 */
  private static void extractGroups(Geometry geom, List<List<CoordinateSequence>> groups, double minSize) {
    if (geom.isEmpty()) {
      // ignore empty geometries
    } else if (geom instanceof GeometryCollection) {
      for (int i = 0; i < geom.getNumGeometries(); i++) {
        extractGroups(geom.getGeometryN(i), groups, minSize);
      }
    } else if (geom instanceof Polygon polygon) {
      extractGroupsFromPolygon(groups, minSize, polygon);
    } else if (geom instanceof LinearRing linearRing) {
      extractGroups(GeoUtils.JTS_FACTORY.createPolygon(linearRing), groups, minSize);
    } else if (geom instanceof LineString lineString) {
      if (lineString.getLength() >= minSize) {
        groups.add(List.of(lineString.getCoordinateSequence()));
      }
    } else {
      throw new IllegalArgumentException("unrecognized geometry type: " + geom.getGeometryType());
    }
  }

  /** Accumulates outer/inner rings over {@code minArea} into {@code groups}. */
  private static void extractGroupsFromPolygon(List<List<CoordinateSequence>> groups, double minArea, Polygon polygon) {
    CoordinateSequence outer = polygon.getExteriorRing().getCoordinateSequence();
    double outerArea = Area.ofRingSigned(outer);
    // ensure CCW winding
    if (outerArea > 0) {
      CoordinateSequences.reverse(outer);
    }
    if (Math.abs(outerArea) >= minArea) {
      List<CoordinateSequence> group = new ArrayList<>(1 + polygon.getNumInteriorRing());
      groups.add(group);
      group.add(outer);
      for (int i = 0; i < polygon.getNumInteriorRing(); i++) {
        CoordinateSequence inner = polygon.getInteriorRingN(i).getCoordinateSequence();
        double innerArea = Area.ofRingSigned(inner);
        // use CCW winding for inner rings as well, subsequent processing will reverse them
        if (innerArea > 0) {
          CoordinateSequences.reverse(inner);
        }
        if (Math.abs(innerArea) >= minArea) {
          group.add(inner);
        }
      }
    }
  }

  /** Returns a {@link LineString} or {@link MultiLineString} containing all coordinate sequences in {@code geoms}. */
  static Geometry reassembleLineStrings(List<List<CoordinateSequence>> geoms) {
    List<LineString> lineStrings = new ArrayList<>();
    for (List<CoordinateSequence> inner : geoms) {
      for (CoordinateSequence coordinateSequence : inner) {
        if (coordinateSequence.size() > 1) {
          lineStrings.add(GeoUtils.JTS_FACTORY.createLineString(coordinateSequence));
        }
      }
    }
    return GeoUtils.combineLineStrings(lineStrings);
  }


  /**
   * Returns a {@link Polygon} or {@link MultiPolygon} from all groups of exterior/interior rings in {@code groups}.
   *
   * @param groups a list of polygons where the first element in each inner list is the exterior ring and subsequent
   *               elements are inner rings.
   * @return the {@link Polygon} or {@link MultiPolygon}
   * @throws GeometryException if rings are not closed or have too few points
   */
  static Geometry reassemblePolygons(List<List<CoordinateSequence>> groups) throws GeometryException {
    int numGeoms = groups.size();
    if (numGeoms == 1) {
      return reassemblePolygon(groups.getFirst());
    } else {
      Polygon[] polygons = new Polygon[numGeoms];
      for (int i = 0; i < numGeoms; i++) {
        polygons[i] = reassemblePolygon(groups.get(i));
      }
      return GeoUtils.JTS_FACTORY.createMultiPolygon(polygons);
    }
  }

  /** Returns a {@link Polygon} built from all outer/inner rings in {@code group}, reversing all inner rings. */
  private static Polygon reassemblePolygon(List<CoordinateSequence> group) throws GeometryException {
    try {
      LinearRing first = GeoUtils.JTS_FACTORY.createLinearRing(group.getFirst());
      LinearRing[] rest = new LinearRing[group.size() - 1];
      for (int j = 1; j < group.size(); j++) {
        CoordinateSequence seq = group.get(j);
        CoordinateSequences.reverse(seq);
        rest[j - 1] = GeoUtils.JTS_FACTORY.createLinearRing(seq);
      }
      return GeoUtils.JTS_FACTORY.createPolygon(first, rest);
    } catch (IllegalArgumentException e) {
      throw new GeometryException("reassemble_polygon_failed", "Could not build polygon", e);
    }
  }

  /** Returns a {@link Polygon} built from all outer/inner rings in {@code group}, reversing all inner rings. */
  static Geometry reassemblePoints(List<List<CoordinateSequence>> result) {
    List<Point> points = new ArrayList<>();
    for (List<CoordinateSequence> inner : result) {
      for (CoordinateSequence coordinateSequence : inner) {
        if (coordinateSequence.size() == 1) {
          points.add(GeoUtils.JTS_FACTORY.createPoint(coordinateSequence));
        }
      }
    }
    return GeoUtils.combinePoints(points);
  }
}
