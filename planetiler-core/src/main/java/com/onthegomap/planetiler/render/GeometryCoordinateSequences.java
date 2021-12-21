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
 *   <li>For linestrings: {@code [[linestring], [linestring], ...]}</li> for each linestring in the collection
 *   <li>For polygons: {@code [[outer ring, inner ring, inner ring], [outer ring, inner ring, ...], ...]}</li> for each
 *   polygon in the multipolygon
 * </ul>
 */
class GeometryCoordinateSequences {

  /**
   * Returns the coordinate sequences extracted from every linear component in {@code geom} over a minimum size
   * threshold.
   * <p>
   * For {@link LineString LineStrings} that means all linestrings over a certain length.
   * <p>
   * For {@link Polygon Polygons} that means all lists of [exterior, interior...] ring coordinate sequences where the
   * ring is over a certain area.  This utility also ensures that exterior and interior rings use counter-clockwise
   * winding.
   *
   * @param geom    one or more linestings or polygons
   * @param minSize minimum length of linestrings, or minimum area of exterior/interior rings to include
   * @return the coordinate sequences of the geometry
   * @throws IllegalArgumentException if {@code geom} contains anything other than linestrings or polygons (i.e.
   *                                  points)
   */
  static List<List<CoordinateSequence>> extractGroups(Geometry geom, double minSize) {
    List<List<CoordinateSequence>> result = new ArrayList<>();
    extractGroups(geom, result, minSize);
    return result;
  }

  /** Accumulates linear components we find over {@code minSize} into {@code groups}. */
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
      return reassemblePolygon(groups.get(0));
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
      LinearRing first = GeoUtils.JTS_FACTORY.createLinearRing(group.get(0));
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
