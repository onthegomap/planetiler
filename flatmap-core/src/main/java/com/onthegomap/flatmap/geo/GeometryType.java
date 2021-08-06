package com.onthegomap.flatmap.geo;

import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Lineal;
import org.locationtech.jts.geom.Polygonal;
import org.locationtech.jts.geom.Puntal;
import vector_tile.VectorTile;

public enum GeometryType {
  UNKNOWN(VectorTile.Tile.GeomType.UNKNOWN),
  POINT(VectorTile.Tile.GeomType.POINT),
  LINE(VectorTile.Tile.GeomType.LINESTRING),
  POLYGON(VectorTile.Tile.GeomType.POLYGON);

  private final VectorTile.Tile.GeomType protobufType;

  GeometryType(VectorTile.Tile.GeomType protobufType) {
    this.protobufType = protobufType;
  }

  public static GeometryType valueOf(Geometry geom) {
    return geom instanceof Puntal ? POINT
      : geom instanceof Lineal ? LINE
        : geom instanceof Polygonal ? POLYGON
          : UNKNOWN;
  }

  public static GeometryType valueOf(VectorTile.Tile.GeomType geomType) {
    return switch (geomType) {
      case POINT -> POINT;
      case LINESTRING -> LINE;
      case POLYGON -> POLYGON;
      default -> UNKNOWN;
    };
  }

  public static GeometryType valueOf(byte val) {
    return valueOf(VectorTile.Tile.GeomType.forNumber(val));
  }

  public byte asByte() {
    return (byte) protobufType.getNumber();
  }

  public VectorTile.Tile.GeomType asProtobufType() {
    return protobufType;
  }
}
