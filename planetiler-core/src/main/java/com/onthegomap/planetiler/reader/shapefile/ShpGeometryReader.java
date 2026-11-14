package com.onthegomap.planetiler.reader.shapefile;

import static com.onthegomap.planetiler.geo.GeoUtils.JTS_FACTORY;

import java.util.ArrayList;
import java.util.List;
import org.locationtech.jts.algorithm.Area;
import org.locationtech.jts.algorithm.Orientation;
import org.locationtech.jts.algorithm.locate.IndexedPointInAreaLocator;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.LinearRing;
import org.locationtech.jts.geom.Location;
import org.locationtech.jts.geom.MultiLineString;
import org.locationtech.jts.geom.MultiPoint;
import org.locationtech.jts.geom.MultiPolygon;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;

/**
 * Builds the JTS geometry of a {@code .shp} record, following the geometry rules of the <a href=
 * "https://www.esri.com/content/dam/esrisites/sitecore-archive/Files/Pdfs/library/whitepapers/pdfs/shapefile.pdf">ESRI
 * Shapefile Technical Description</a>. Where the values are in the record's bytes is up to {@link ShpRecord}.
 * <p>
 * Points become a {@link Point}, multipoints a {@link MultiPoint}, polylines a {@link MultiLineString} and polygons a
 * {@link MultiPolygon}, with each hole in the outer ring that contains it. Z and M values are kept.
 */
final class ShpGeometryReader {
  private ShpGeometryReader() {}

  /**
   * Returns the geometry of a record.
   *
   * @param content the record shpRecord after its {@link ShpRecordHeader}, starting with the shape type
   * @throws IllegalArgumentException if the record has an unsupported shape type or no usable geometry
   */
  static Geometry read(byte[] content) {
    var shpRecord = ShpRecord.of(content);
    var type = shpRecord.type();
    if (type.isPointType()) {
      return JTS_FACTORY.createPoint(shpRecord.coordinates(0, 1, false));
    } else if (type.isMultiPointType()) {
      Point[] points = new Point[shpRecord.numPoints()];
      for (int i = 0; i < points.length; i++) {
        points[i] = JTS_FACTORY.createPoint(shpRecord.coordinates(i, i + 1, false));
      }
      return JTS_FACTORY.createMultiPoint(points);
    }
    // Polylines and polygons are always multi geometries, even with one part, to match legacy GeoTools behavior.
    // TODO return a LineString / Polygon when there is only one part
    return type.isLineType() ? multiLineString(shpRecord) : multiPolygon(shpRecord);
  }

  private static MultiLineString multiLineString(ShpRecord shpRecord) {
    LineString[] lines = new LineString[shpRecord.numParts()];
    for (int part = 0; part < lines.length; part++) {
      int from = shpRecord.partStart(part);
      int to = shpRecord.partEnd(part);
      // a part needs 2 points to be a line, so repeat a lone point
      lines[part] = JTS_FACTORY.createLineString(shpRecord.coordinates(from, to, to - from == 1));
    }
    return JTS_FACTORY.createMultiLineString(lines);
  }

  private static MultiPolygon multiPolygon(ShpRecord shpRecord) {
    List<Shell> shells = new ArrayList<>();
    List<LinearRing> holes = new ArrayList<>();
    for (int part = 0; part < shpRecord.numParts(); part++) {
      LinearRing ring = readRing(shpRecord, shpRecord.partStart(part), shpRecord.partEnd(part));
      if (ring != null) {
        // from spec "Polygon": outer rings are clockwise, holes counterclockwise, and ring order is not significant.
        if (Orientation.isCCW(ring.getCoordinateSequence())) {
          holes.add(ring);
        } else {
          shells.add(new Shell(ring));
        }
      }
    }
    if (shells.isEmpty() && holes.isEmpty()) {
      throw new IllegalArgumentException("Polygon record has no ring with at least 4 points");
    }
    // With one outer ring, every hole belongs to it, so skip the containment tests. A hole outside of it makes the
    // polygon invalid, which is what the record describes.
    Shell onlyShell = shells.size() == 1 ? shells.getFirst() : null;
    for (LinearRing hole : holes) {
      Shell shell = onlyShell != null ? onlyShell : smallestShellContaining(shells, hole);
      if (shell == null) {
        // A hole outside every outer ring is most likely an outer ring with the wrong orientation.
        shells.add(new Shell(hole));
      } else {
        shell.holes.add(hole);
      }
    }
    Polygon[] polygons = new Polygon[shells.size()];
    for (int i = 0; i < polygons.length; i++) {
      Shell shell = shells.get(i);
      polygons[i] = JTS_FACTORY.createPolygon(shell.ring, shell.holes.toArray(LinearRing[]::new));
    }
    return JTS_FACTORY.createMultiPolygon(polygons);
  }

  private static LinearRing readRing(ShpRecord shpRecord, int from, int to) {
    int numPoints = to - from;
    if (numPoints < 3) {
      return null;
    }
    // from spec "Polygon": rings must be closed and have at least 4 points, but close an unclosed ring instead of
    // dropping it.
    boolean closed = shpRecord.samePoint(from, to);
    if ((closed ? numPoints : numPoints + 1) < 4) {
      return null;
    }
    return JTS_FACTORY.createLinearRing(shpRecord.coordinates(from, to, !closed));
  }

  private static Shell smallestShellContaining(List<Shell> shells, LinearRing hole) {
    Shell smallest = null;
    for (Shell shell : shells) {
      // The area is only computed when several outer rings contain the hole, and strict comparison picks the first of
      // exactly equal outer rings.
      if (shell.envelope.covers(hole.getEnvelopeInternal()) && (smallest == null || shell.area() < smallest.area()) &&
        shell.contains(hole)) {
        smallest = shell;
      }
    }
    return smallest;
  }

  private static class Shell {
    final LinearRing ring;
    final Envelope envelope;
    final List<LinearRing> holes = new ArrayList<>();
    private double area = Double.NaN;
    // Built on first use, since most outer rings never get tested, and the index makes testing many holes faster
    private IndexedPointInAreaLocator locator;

    Shell(LinearRing ring) {
      this.ring = ring;
      this.envelope = ring.getEnvelopeInternal();
    }

    double area() {
      if (Double.isNaN(area)) {
        area = Area.ofRing(ring.getCoordinateSequence());
      }
      return area;
    }

    boolean contains(LinearRing hole) {
      if (locator == null) {
        locator = new IndexedPointInAreaLocator(ring);
      }
      int location = locator.locate(hole.getCoordinateN(0));
      if (location == Location.BOUNDARY) {
        // from spec "Polygon": rings may touch at vertices but not cross, so a point inside the hole is either inside
        // this ring or outside, never on it.
        location = locator.locate(hole.getFactory().createPolygon(hole).getInteriorPoint().getCoordinate());
      }
      return location != Location.EXTERIOR;
    }
  }
}
