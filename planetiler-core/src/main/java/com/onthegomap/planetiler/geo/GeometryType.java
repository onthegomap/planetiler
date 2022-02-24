package com.onthegomap.planetiler.geo;

import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Lineal;
import org.locationtech.jts.geom.Polygonal;
import org.locationtech.jts.geom.Puntal;
import vector_tile.VectorTileProto;

public enum GeometryType {
  UNKNOWN(VectorTileProto.Tile.GeomType.UNKNOWN, 0),
  POINT(VectorTileProto.Tile.GeomType.POINT, 1),
  LINE(VectorTileProto.Tile.GeomType.LINESTRING, 2),
  POLYGON(VectorTileProto.Tile.GeomType.POLYGON, 4);

  private final VectorTileProto.Tile.GeomType protobufType;
  private final int minPoints;

  GeometryType(VectorTileProto.Tile.GeomType protobufType, int minPoints) {
    this.protobufType = protobufType;
    this.minPoints = minPoints;
  }

  public static GeometryType valueOf(Geometry geom) {
    return geom instanceof Puntal ? POINT
      : geom instanceof Lineal ? LINE
        : geom instanceof Polygonal ? POLYGON
          : UNKNOWN;
  }

  public static GeometryType valueOf(VectorTileProto.Tile.GeomType geomType) {
    return switch (geomType) {
      case POINT -> POINT;
      case LINESTRING -> LINE;
      case POLYGON -> POLYGON;
      default -> UNKNOWN;
    };
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
}
