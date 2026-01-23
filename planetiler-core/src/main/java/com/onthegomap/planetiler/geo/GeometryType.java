package com.onthegomap.planetiler.geo;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.onthegomap.planetiler.expression.Expression;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Locale;
import java.util.regex.Pattern;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryCollection;
import org.locationtech.jts.geom.Lineal;
import org.locationtech.jts.geom.Polygonal;
import org.locationtech.jts.geom.Puntal;
import vector_tile.VectorTileProto;

public enum GeometryType {
  UNKNOWN(VectorTileProto.Tile.GeomType.UNKNOWN, 0, "unknown") {
    @Override
    public Expression featureTest() {
      return Expression.TRUE;
    }
  },
  @JsonProperty("point")
  POINT(VectorTileProto.Tile.GeomType.POINT, 1, "point"),
  @JsonProperty("line")
  LINE(VectorTileProto.Tile.GeomType.LINESTRING, 2, "linestring"),
  @JsonProperty("polygon")
  POLYGON(VectorTileProto.Tile.GeomType.POLYGON, 4, "polygon");

  private final VectorTileProto.Tile.GeomType protobufType;
  private final int minPoints;
  private final String matchTypeString;

  GeometryType(VectorTileProto.Tile.GeomType protobufType, int minPoints, String matchTypeString) {
    this.protobufType = protobufType;
    this.minPoints = minPoints;
    this.matchTypeString = matchTypeString;
  }

  public static GeometryType typeOf(Geometry geom) {
    if (geom instanceof GeometryCollection collection && collection.getNumGeometries() >= 1) {
      var result = typeOf(collection.getGeometryN(0));
      for (int i = 1; i < collection.getNumGeometries(); i++) {
        if (!result.equals(typeOf(collection.getGeometryN(i)))) {
          return UNKNOWN;
        }
      }
      return result;
    }
    return typeOfPrimitive(geom);
  }

  private static GeometryType typeOfPrimitive(Geometry geom) {
    return geom instanceof Puntal ? POINT : geom instanceof Lineal ? LINE : geom instanceof Polygonal ? POLYGON :
      UNKNOWN;
  }

  public static GeometryType valueOf(VectorTileProto.Tile.GeomType geomType) {
    return switch (geomType) {
      case POINT -> POINT;
      case LINESTRING -> LINE;
      case POLYGON -> POLYGON;
      default -> UNKNOWN;
    };
  }

  /** Returns the type of a WKB-encoded geometry without needing to deserialize the whole thing. */
  public static GeometryType fromWKB(byte[] wkb) {
    var bb = ByteBuffer.wrap(wkb);
    byte byteOrder = bb.get();
    int geomType = bb.order(byteOrder == 1 ? ByteOrder.LITTLE_ENDIAN : ByteOrder.BIG_ENDIAN).getInt();
    return switch (geomType) {
      case 1, 4 -> GeometryType.POINT;
      case 2, 5 -> GeometryType.LINE;
      case 3, 6 -> GeometryType.POLYGON;
      default -> GeometryType.UNKNOWN;
    };
  }

  private static final Pattern TYPE_PATTERN =
    Pattern.compile("^\\s*(multi)?(point|line|polygon)", Pattern.CASE_INSENSITIVE);

  /** Returns the type of a WKT-encoded geometry without needing to deserialize the whole thing. */
  public static GeometryType fromWKT(String wkt) {
    var matcher = TYPE_PATTERN.matcher(wkt);
    if (matcher.find()) {
      String group = matcher.group(2);
      if (group != null) {
        return switch (group.toLowerCase(Locale.ROOT)) {
          case "point" -> GeometryType.POINT;
          case "line" -> GeometryType.LINE;
          case "polygon" -> GeometryType.POLYGON;
          default -> GeometryType.UNKNOWN;
        };
      }
    }
    return GeometryType.UNKNOWN;
  }

  public static GeometryType valueOf(byte val) {
    return valueOf(VectorTileProto.Tile.GeomType.forNumber(val));
  }

  public byte asByte() {
    return (byte) protobufType.getNumber();
  }

  public VectorTileProto.Tile.GeomType asProtobufType() {
    return protobufType;
  }

  public int minPoints() {
    return minPoints;
  }

  /**
   * Generates a test for whether a source feature is of the correct geometry to be included in the tile.
   *
   * @return geometry test method
   */
  public Expression featureTest() {
    return Expression.matchType(matchTypeString);
  }

}
