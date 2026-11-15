package com.onthegomap.planetiler.reader.shapefile;

import static com.onthegomap.planetiler.geo.GeoUtils.JTS_FACTORY;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.ToDoubleBiFunction;
import org.locationtech.jts.algorithm.Orientation;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.CoordinateSequence;
import org.locationtech.jts.geom.CoordinateSequences;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.MultiLineString;
import org.locationtech.jts.geom.MultiPoint;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.geom.Polygonal;

/** Writes small shapefiles ({@code .shp}, {@code .shx}, {@code .dbf}, {@code .cpg}, {@code .prj}) for tests. */
public final class TestShapefileWriter {
  public static final String WGS84_PRJ =
    "GEOGCS[\"GCS_WGS_1984\",DATUM[\"D_WGS_1984\",SPHEROID[\"WGS_1984\",6378137.0,298.257223563]]," +
      "PRIMEM[\"Greenwich\",0.0],UNIT[\"Degree\",0.0174532925199433]]";

  private TestShapefileWriter() {}

  /** Returns a feature whose geometry can't be read: a polygon record that ends right after its shape type. */
  public static ShapefileFeature unreadableFeature(String sourceLayer, long id) {
    return new ShapefileFeature(new byte[]{5, 0, 0, 0}, null, Map.of(), "sourceName", sourceLayer, id);
  }

  /**
   * Writes {@code geometries} (all points, multipoints, lines or polygons, with or without Z / M) with the attributes in
   * {@code rows} ({@link Integer} values or strings of up to 9 bytes, in {@code fieldNames} order).
   */
  public static Path write(Path shpPath, String prj, List<Geometry> geometries, List<String> fieldNames,
    List<List<Object>> rows) throws IOException {
    ShapeType type = shapeType(geometries.getFirst());
    var envelope = JTS_FACTORY.createGeometryCollection(geometries.toArray(Geometry[]::new)).getEnvelopeInternal();
    var shp = new ByteArrayOutputStream();
    var shx = new ByteArrayOutputStream();
    List<byte[]> records = geometries.stream().map(geometry -> content(type, geometry)).toList();
    shp.write(header(type, 100 + records.stream().mapToInt(r -> 8 + r.length).sum(), envelope));
    shx.write(header(type, 100 + 8 * records.size(), envelope));
    for (int i = 0; i < records.size(); i++) {
      byte[] content = records.get(i);
      shx.write(ByteBuffer.allocate(8).putInt(shp.size() / 2).putInt(content.length / 2).array());
      shp.write(ByteBuffer.allocate(8).putInt(i + 1).putInt(content.length / 2).array());
      shp.write(content);
    }
    String base = shpPath.toString().replaceAll("\\.shp$", "");
    Files.write(shpPath, shp.toByteArray());
    Files.write(Path.of(base + ".shx"), shx.toByteArray());
    Files.write(Path.of(base + ".dbf"), dbf(fieldNames, rows));
    Files.writeString(Path.of(base + ".cpg"), "UTF-8");
    if (prj != null) {
      Files.writeString(Path.of(base + ".prj"), prj);
    }
    return shpPath;
  }

  /** Writes {@code geometries} in WGS84, the i-th with {@code value} i + 1. */
  public static Path write(Path shpPath, List<Geometry> geometries) throws IOException {
    List<List<Object>> rows = new ArrayList<>();
    for (int i = 0; i < geometries.size(); i++) {
      rows.add(List.of(i + 1));
    }
    return write(shpPath, WGS84_PRJ, geometries, List.of("value"), rows);
  }

  private static ShapeType shapeType(Geometry geometry) {
    var base = switch (geometry) {
      case Point _ -> ShapeType.POINT;
      case MultiPoint _ -> ShapeType.MULTIPOINT;
      case LineString _, MultiLineString _ -> ShapeType.POLYLINE;
      case Polygonal _ -> ShapeType.POLYGON;
      default -> throw new IllegalArgumentException("Unsupported " + geometry.getGeometryType());
    };
    // from spec "Shape Types": the Z variant of each type is 10 more, the M variant 20 more
    Coordinate first = geometry.getCoordinate();
    return ShapeType.forId(base.id + (!Double.isNaN(first.getZ()) ? 10 : !Double.isNaN(first.getM()) ? 20 : 0));
  }

  private static byte[] header(ShapeType type, int fileBytes, Envelope envelope) {
    var header = ByteBuffer.allocate(100);
    header.putInt(0, 9994).putInt(24, fileBytes / 2);
    header.order(ByteOrder.LITTLE_ENDIAN).putInt(28, 1000).putInt(32, type.id)
      .putDouble(36, envelope.getMinX()).putDouble(44, envelope.getMinY())
      .putDouble(52, envelope.getMaxX()).putDouble(60, envelope.getMaxY());
    return header.array();
  }

  private static byte[] content(ShapeType type, Geometry geometry) {
    if (type.isPointType()) {
      Coordinate c = geometry.getCoordinate();
      boolean z = type.hasZ();
      boolean m = !Double.isNaN(c.getM());
      var buffer = ByteBuffer.allocate(20 + (z ? 8 : 0) + (m ? 8 : 0)).order(ByteOrder.LITTLE_ENDIAN).putInt(type.id)
        .putDouble(c.x).putDouble(c.y);
      if (z) {
        buffer.putDouble(c.getZ());
      }
      if (m) {
        buffer.putDouble(c.getM());
      }
      return buffer.array();
    }
    List<CoordinateSequence> parts = new ArrayList<>();
    for (int i = 0; i < geometry.getNumGeometries(); i++) {
      switch (geometry.getGeometryN(i)) {
        case Polygon polygon -> {
          // outer rings are clockwise and holes counterclockwise
          parts.add(oriented(polygon.getExteriorRing().getCoordinateSequence(), false));
          for (int h = 0; h < polygon.getNumInteriorRing(); h++) {
            parts.add(oriented(polygon.getInteriorRingN(h).getCoordinateSequence(), true));
          }
        }
        case LineString line -> parts.add(line.getCoordinateSequence());
        case Point point -> parts.add(point.getCoordinateSequence());
        default -> throw new IllegalArgumentException("Unsupported " + geometry.getGeometryType());
      }
    }
    return partsRecord(type, parts);
  }

  /**
   * Returns the contents of a multipoint, polyline or polygon record (or their Z / M variants, with m values if
   * {@code parts} have them) with {@code parts} written in order and as given, without closing or reorienting rings,
   * so tests can describe records a JTS geometry can't.
   */
  public static byte[] partsRecord(ShapeType type, List<CoordinateSequence> parts) {
    boolean z = type.hasZ();
    boolean m = parts.stream().anyMatch(CoordinateSequence::hasM);
    boolean multiPoint = type.isMultiPointType();
    int numPoints = parts.stream().mapToInt(CoordinateSequence::size).sum();
    int numParts = multiPoint ? 0 : parts.size();
    int size = 4 + 32 + 4 + (multiPoint ? 0 : 4) + 4 * numParts + 16 * numPoints + (z ? 16 + 8 * numPoints : 0) +
      (m ? 16 + 8 * numPoints : 0);
    var envelope = new Envelope();
    parts.forEach(part -> part.expandEnvelope(envelope));
    var buffer = ByteBuffer.allocate(size).order(ByteOrder.LITTLE_ENDIAN).putInt(type.id)
      .putDouble(envelope.getMinX()).putDouble(envelope.getMinY())
      .putDouble(envelope.getMaxX()).putDouble(envelope.getMaxY());
    if (!multiPoint) {
      buffer.putInt(numParts);
    }
    buffer.putInt(numPoints);
    int first = 0;
    for (int i = 0; i < numParts; i++) {
      buffer.putInt(first);
      first += parts.get(i).size();
    }
    for (var part : parts) {
      for (int i = 0; i < part.size(); i++) {
        buffer.putDouble(part.getX(i)).putDouble(part.getY(i));
      }
    }
    if (z) {
      putRangeAndValues(buffer, parts, CoordinateSequence::getZ);
    }
    if (m) {
      putRangeAndValues(buffer, parts, CoordinateSequence::getM);
    }
    return buffer.array();
  }

  private static void putRangeAndValues(ByteBuffer buffer, List<CoordinateSequence> parts,
    ToDoubleBiFunction<CoordinateSequence, Integer> value) {
    // the reader ignores the min / max range
    buffer.putDouble(0).putDouble(0);
    for (var part : parts) {
      for (int i = 0; i < part.size(); i++) {
        buffer.putDouble(value.applyAsDouble(part, i));
      }
    }
  }

  private static CoordinateSequence oriented(CoordinateSequence ring, boolean ccw) {
    if (Orientation.isCCW(ring) != ccw) {
      ring = ring.copy();
      CoordinateSequences.reverse(ring);
    }
    return ring;
  }

  /** A {@code .dbf} field descriptor: {@code type} is the dBase type letter ({@code C}, {@code N}, ...). */
  public record Field(String name, char type, int length, int decimals) {}

  /**
   * Returns a dBase III file with {@code fields} and {@code records} as written: each record is the deletion flag
   * ({@code ' '} or {@code '*'}) followed by every field's padded text, encoded with {@code charset}.
   */
  public static byte[] dbf(List<Field> fields, Charset charset, List<String> records) {
    int recordLength = 1 + fields.stream().mapToInt(Field::length).sum();
    int headerLength = 32 + 32 * fields.size() + 1;
    var buffer = ByteBuffer.allocate(headerLength + recordLength * records.size() + 1).order(ByteOrder.LITTLE_ENDIAN);
    buffer.put((byte) 3).put(new byte[3]).putInt(records.size()).putShort((short) headerLength)
      .putShort((short) recordLength).put(new byte[20]);
    for (var field : fields) {
      buffer.put(Arrays.copyOf(field.name().getBytes(charset), 11)).put((byte) field.type()).putInt(0)
        .put((byte) field.length()).put((byte) field.decimals()).put(new byte[14]);
    }
    buffer.put((byte) 0x0D);
    for (String nextRecord : records) {
      byte[] bytes = nextRecord.getBytes(charset);
      if (bytes.length != recordLength) {
        throw new IllegalArgumentException(bytes.length + " bytes instead of " + recordLength + ": " + nextRecord);
      }
      buffer.put(bytes);
    }
    return buffer.put((byte) 0x1A).array();
  }

  private static byte[] dbf(List<String> fieldNames, List<List<Object>> rows) {
    // 9 characters so numbers read back as Integer
    int width = 9;
    List<Field> fields = new ArrayList<>();
    for (int f = 0; f < fieldNames.size(); f++) {
      boolean number = !rows.isEmpty() && rows.getFirst().get(f) instanceof Integer;
      fields.add(new Field(fieldNames.get(f), number ? 'N' : 'C', width, 0));
    }
    List<String> records = new ArrayList<>();
    for (var row : rows) {
      var nextRecord = new StringBuilder(" ");
      for (Object value : row) {
        String text = String.valueOf(value);
        String padding = " ".repeat(width - text.getBytes(StandardCharsets.UTF_8).length);
        // numbers are right aligned, text left aligned
        nextRecord.append(value instanceof Integer ? padding + text : text + padding);
      }
      records.add(nextRecord.toString());
    }
    return dbf(fields, StandardCharsets.UTF_8, records);
  }
}
