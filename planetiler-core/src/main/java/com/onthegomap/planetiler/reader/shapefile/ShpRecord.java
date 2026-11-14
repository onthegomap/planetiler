package com.onthegomap.planetiler.reader.shapefile;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.impl.PackedCoordinateSequence;

/**
 * The bytes of one {@code .shp} record after its {@link ShpRecordHeader}, from spec "Main File Record Contents" of the
 * <a href=
 * "https://www.esri.com/content/dam/esrisites/sitecore-archive/Files/Pdfs/library/whitepapers/pdfs/shapefile.pdf">ESRI
 * Shapefile Technical Description</a>.
 * <p>
 * This class only knows where each value is in those bytes. {@link #of(byte[])} finds the points and parts of a record
 * so {@link ShpGeometryReader} can read them one at a time to build a geometry, and the static helpers read a single
 * field without reading the rest.
 */
final class ShpRecord {
  // Little endian, offsets in bytes, laid out for the Z types. The M types have no z fields, so their m fields come
  // right after the points. The m fields are optional in the Z types.
  //
  // Point, PointZ:
  //    +------------+-----+
  //  0 | shape type | i32 |
  //  4 | x          | f64 |
  // 12 | y          | f64 |
  // 20 | z          | f64 |
  // 28 | m          | f64 |
  //    +------------+-----+
  //
  // MultiPoint, MultiPointZ, with N = numPoints:
  //        +------------+----------------+
  //      0 | shape type | i32            |
  //      4 | xmin       | f64            |
  //     12 | ymin       | f64            |
  //     20 | xmax       | f64            |
  //     28 | ymax       | f64            |
  //     36 | numPoints  | i32            |
  //     40 | points     | N x (x, y f64) |
  // 40+16N | zmin       | f64            |
  // 48+16N | zmax       | f64            |
  // 56+16N | z values   | N x f64        |
  // 56+24N | mmin       | f64            |
  // 64+24N | mmax       | f64            |
  // 72+24N | m values   | N x f64        |
  //        +------------+----------------+
  //
  // PolyLine, PolyLineZ, Polygon, PolygonZ, with N = numPoints, K = numParts and P = 44 + 4K:
  //          +-----------------------------+----------------+
  //        0 | shape type                  | i32            |
  //        4 | xmin                        | f64            |
  //       12 | ymin                        | f64            |
  //       20 | xmax                        | f64            |
  //       28 | ymax                        | f64            |
  //       36 | numParts (K)                | i32            |
  //       40 | numPoints (N)               | i32            |
  //       44 | parts: index of first point | K x i32        |
  //        P | points                      | N x (x, y f64) |
  //    P+16N | zmin                        | f64            |
  //  P+8+16N | zmax                        | f64            |
  // P+16+16N | z values                    | N x f64        |
  // P+16+24N | mmin                        | f64            |
  // P+24+24N | mmax                        | f64            |
  // P+32+24N | m values                    | N x f64        |
  //          +-----------------------------+----------------+

  private final ByteBuffer buffer;
  private final ShapeType type;
  private final int numParts;
  private final int numPoints;
  private final int pointsStart;
  // where the z values start, or -1 if there are none
  private final int zStart;
  // where the m values start, or -1 if there are none
  private final int mStart;

  private ShpRecord(byte[] content) {
    buffer = ByteBuffer.wrap(content).order(ByteOrder.LITTLE_ENDIAN);
    type = ShapeType.forId(buffer.getInt(0));
    boolean hasZ = type.hasZ();
    if (type.isPointType()) {
      numParts = 0;
      numPoints = 1;
      pointsStart = 4;
      zStart = hasZ ? 20 : -1;
    } else if (type.isMultiPointType()) {
      numParts = 0;
      numPoints = buffer.getInt(36);
      checkPartsAndPoints(buffer, 0, numPoints, 40);
      pointsStart = 40;
      zStart = hasZ ? 56 + 16 * numPoints : -1;
    } else if (type.isLineType() || type.isPolygonType()) {
      numParts = buffer.getInt(36);
      numPoints = buffer.getInt(40);
      checkPartsAndPoints(buffer, numParts, numPoints, 44);
      pointsStart = 44 + 4 * numParts;
      zStart = hasZ ? pointsStart + 16 + 16 * numPoints : -1;
    } else {
      throw new IllegalArgumentException("Unsupported shape type " + type);
    }
    // m values follow the z values, or the points if there are none, after a min / max range except in points
    int afterCoordinates = hasZ ? zStart + 8 * numPoints : pointsStart + 16 * numPoints;
    int mValuesStart = afterCoordinates + (type.isPointType() ? 0 : 16);
    boolean hasM = type.canHaveM() && mValuesStart + 8L * numPoints <= content.length;
    mStart = hasM ? mValuesStart : -1;
  }

  private static void checkPartsAndPoints(ByteBuffer buffer, int numParts, int numPoints, int partsStart) {
    // Make sure a corrupt count or part index doesn't try to read past the end of the buffer. Otherwise we would
    // allocate an array that could crash the JVM instead of failing on this one feature.
    long pointsEnd = partsStart + 4L * numParts + 16L * numPoints;
    if (numParts < 0 || numPoints < 0 || pointsEnd > buffer.capacity()) {
      throw new IllegalArgumentException("Invalid " + numParts + " parts and " + numPoints +
        " points for a record of only " + buffer.capacity() + " bytes");
    }
    for (int part = 0; part < numParts; part++) {
      int firstPoint = buffer.getInt(partsStart + 4 * part);
      if (firstPoint < 0 || firstPoint > numPoints) {
        throw new IllegalArgumentException("Invalid point " + firstPoint + ", expected " + numPoints);
      }
    }
  }

  /**
   * Returns the record in {@code content}, which must have a point, multipoint, polyline or polygon shape type.
   *
   * @throws IllegalArgumentException if the shape type is unknown, null or multipatch, or the record is too short for
   *                                  its parts and points
   */
  static ShpRecord of(byte[] content) {
    return new ShpRecord(content);
  }

  /** Returns whether a record is long enough to have a shape type. */
  static boolean hasShapeType(byte[] content) {
    return content.length >= 4;
  }

  /** Returns the shape type of a record, or {@link ShapeType#NULL} if it has none or it is unknown. */
  static ShapeType parseShapeType(byte[] content) {
    if (!hasShapeType(content)) {
      return ShapeType.NULL;
    }
    try {
      return ShapeType.forId(ByteBuffer.wrap(content).order(ByteOrder.LITTLE_ENDIAN).getInt(0));
    } catch (IllegalArgumentException _) {
      return ShapeType.NULL;
    }
  }

  /** Returns the number of points in a record, or 1 if it can't be read. */
  static int parseNumPoints(byte[] content) {
    var type = parseShapeType(content);
    int offset;
    if (type.isMultiPointType()) {
      offset = 36;
    } else if (type.isLineType() || type.isPolygonType()) {
      offset = 40;
    } else {
      return 1;
    }
    if (content.length < offset + 4) {
      return 1;
    }
    int numPoints = ByteBuffer.wrap(content).order(ByteOrder.LITTLE_ENDIAN).getInt(offset);
    return Math.max(1, numPoints);
  }

  /**
   * Returns the bounding box of a record, an empty envelope if it has no shape, or {@code null} if it can't be read.
   */
  static Envelope parseBounds(byte[] content) {
    if (!hasShapeType(content)) {
      return new Envelope();
    }
    var buffer = ByteBuffer.wrap(content).order(ByteOrder.LITTLE_ENDIAN);
    ShapeType type;
    try {
      type = ShapeType.forId(buffer.getInt(0));
    } catch (IllegalArgumentException _) {
      return null;
    }
    if ( type == ShapeType.NULL) {
      return new Envelope();
    } else if (type.isPointType()) {
      if (content.length < 20) {
        // From spec "Point": the shape type (4 bytes) is followed by x and y (8 bytes each). A shorter record has no
        // point to compare against the bounds.
        return null;
      }
      double x = buffer.getDouble(4);
      double y = buffer.getDouble(12);
      return new Envelope(x, x, y, y);
    } else if (content.length < 36) {
      // From spec "MultiPoint" / "PolyLine" / "Polygon": the shape type (4 bytes) is followed by the bounding box (4
      // doubles, 32 bytes). A shorter record has no bounding box.
      return null;
    }
    double xmin = buffer.getDouble(4);
    double ymin = buffer.getDouble(12);
    double xmax = buffer.getDouble(20);
    double ymax = buffer.getDouble(28);
    return new Envelope(xmin, xmax, ymin, ymax);
  }

  ShapeType type() {
    return type;
  }

  int numParts() {
    return numParts;
  }

  int numPoints() {
    return numPoints;
  }

  /** Returns the index of the first point of {@code part}. */
  int partStart(int part) {
    return buffer.getInt(44 + 4 * part);
  }

  /** Returns the index after the last point of {@code part}. */
  int partEnd(int part) {
    return part + 1 < numParts ? buffer.getInt(48 + 4 * part) : numPoints;
  }

  /** Returns whether the points {@code from} and {@code to - 1} have the same x and y. */
  boolean samePoint(int from, int to) {
    int first = pointsStart + 16 * from;
    int last = pointsStart + 16 * (to - 1);
    return buffer.getDouble(first) == buffer.getDouble(last) &&
      buffer.getDouble(first + 8) == buffer.getDouble(last + 8);
  }

  /**
   * Returns points {@code from} (inclusive) to {@code to} (exclusive), with z and m values if the record has them,
   * optionally closed by repeating the first point.
   */
  PackedCoordinateSequence.Double coordinates(int from, int to, boolean close) {
    int measures = mStart >= 0 ? 1 : 0;
    int dimension = 2 + (zStart >= 0 ? 1 : 0) + measures;
    int count = to - from + (close ? 1 : 0);
    double[] coords = new double[dimension * count];
    int idx = 0;
    for (int i = 0; i < to - from; i++) {
      int point = from + i;
      coords[idx] = buffer.getDouble(pointsStart + 16 * point);
      coords[idx + 1] = buffer.getDouble(pointsStart + 16 * point + 8);
      if (zStart >= 0) {
        coords[idx + 2] = buffer.getDouble(zStart + 8 * point);
      }
      if (mStart >= 0) {
        // from spec "Numeric Types": values below -10^38 mean no data
        double m = buffer.getDouble(mStart + 8 * point);
        coords[idx + dimension - 1] = m < -1e38 ? Double.NaN : m;
      }
      idx += dimension;
    }
    if (close) {
      System.arraycopy(coords, 0, coords, coords.length - dimension, dimension);
    }
    return new PackedCoordinateSequence.Double(coords, dimension, measures);
  }
}
