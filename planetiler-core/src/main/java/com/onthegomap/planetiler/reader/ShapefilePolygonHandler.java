package com.onthegomap.planetiler.reader;

import static com.onthegomap.planetiler.geo.GeoUtils.JTS_FACTORY;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import org.geotools.data.shapefile.shp.PolygonHandler;
import org.geotools.data.shapefile.shp.ShapeHandler;
import org.geotools.data.shapefile.shp.ShapeType;
import org.locationtech.jts.algorithm.Area;
import org.locationtech.jts.algorithm.Orientation;
import org.locationtech.jts.algorithm.locate.IndexedPointInAreaLocator;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.LinearRing;
import org.locationtech.jts.geom.Location;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.geom.impl.PackedCoordinateSequence;

/**
 * Reads 2D polygon records from a shapefile into a JTS {@code MultiPolygon}, following the "Polygon" section of the
 * <a href=
 * "https://www.esri.com/content/dam/esrisites/sitecore-archive/Files/Pdfs/library/whitepapers/pdfs/shapefile.pdf">ESRI
 * Shapefile Technical Description</a>.
 * <p>
 * This serves a similar function as {@link PolygonHandler} except it is much faster assigning holes to shells since it
 * creates a spatial index to speed up that lookup when there are many.
 */
class ShapefilePolygonHandler implements ShapeHandler {
  // Clockwise rings are outer rings and counterclockwise rings are holes. With one outer ring, every hole goes into it.
  // With several, each hole goes into the smallest outer ring that contains it (a hole may touch its outer ring at a
  // vertex), and a hole that no outer ring contains becomes an outer ring of its own. Polygons come out in record order
  // of their outer rings followed by the ones from such orphaned holes, each with its holes in record order. Unclosed
  // rings get closed, and rings with fewer than 4 points are skipped. Other shape types (null, Z, M) and writing are
  // delegated to GeoTools.

  private final PolygonHandler delegate;

  ShapefilePolygonHandler() {
    this.delegate = new PolygonHandler(JTS_FACTORY);
  }

  @Override
  public ShapeType getShapeType() {
    return ShapeType.POLYGON;
  }

  @Override
  public void write(ByteBuffer buffer, Object geometry) {
    delegate.write(buffer, geometry);
  }

  @Override
  public int getLength(Object geometry) {
    return delegate.getLength(geometry);
  }

  @Override
  public Object read(ByteBuffer buffer, ShapeType type, boolean flatFeature) {
    if (type != ShapeType.POLYGON) {
      return delegate.read(buffer, type, flatFeature);
    }
    // from spec "Polygon Record Contents": the buffer starts after the shape type (byte 4) and is little endian, as
    // "Organization of the Main File" requires for record contents. The 32 byte bounding box isn't needed.
    int start = buffer.position();
    int numParts = buffer.getInt(start + 32);
    int numPoints = buffer.getInt(start + 36);
    int pointsStart = start + 40 + 4 * numParts;
    List<Shell> shells = new ArrayList<>();
    List<LinearRing> holes = new ArrayList<>();
    for (int part = 0; part < numParts; part++) {
      int from = buffer.getInt(start + 40 + 4 * part);
      int to = part + 1 < numParts ? buffer.getInt(start + 44 + 4 * part) : numPoints;
      LinearRing ring = readRing(buffer, pointsStart, from, to);
      if (ring == null) {
        continue;
      }
      // from spec "Polygon": outer rings are clockwise, holes counterclockwise, and ring order is not significant.
      if (Orientation.isCCW(ring.getCoordinateSequence())) {
        holes.add(ring);
      } else {
        shells.add(new Shell(ring));
      }
    }
    buffer.position(pointsStart + 16 * numPoints);
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

  private LinearRing readRing(ByteBuffer buffer, int pointsStart, int from, int to) {
    int numPoints = to - from;
    if (numPoints < 3) {
      return null;
    }
    int firstOffset = pointsStart + 16 * from;
    int lastOffset = pointsStart + 16 * (to - 1);
    // from spec "Polygon": rings must be closed and have at least 4 points, but close an unclosed ring instead of
    // dropping it.
    boolean closed = buffer.getDouble(firstOffset) == buffer.getDouble(lastOffset) &&
      buffer.getDouble(firstOffset + 8) == buffer.getDouble(lastOffset + 8);
    int ringPoints = closed ? numPoints : numPoints + 1;
    if (ringPoints < 4) {
      return null;
    }
    double[] coords = new double[2 * ringPoints];
    for (int i = 0; i < 2 * numPoints; i++) {
      coords[i] = buffer.getDouble(firstOffset + 8 * i);
    }
    if (!closed) {
      coords[2 * numPoints] = coords[0];
      coords[2 * numPoints + 1] = coords[1];
    }
    return JTS_FACTORY.createLinearRing(new PackedCoordinateSequence.Double(coords, 2, 0));
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
