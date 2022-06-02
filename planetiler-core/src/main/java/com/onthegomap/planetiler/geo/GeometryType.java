package com.onthegomap.planetiler.geo;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.onthegomap.planetiler.FeatureCollector;
import com.onthegomap.planetiler.FeatureCollector.Feature;
import com.onthegomap.planetiler.expression.Expression;
import java.util.function.BiFunction;
import java.util.function.Function;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Lineal;
import org.locationtech.jts.geom.Polygonal;
import org.locationtech.jts.geom.Puntal;
import vector_tile.VectorTileProto;

public enum GeometryType {
  UNKNOWN(VectorTileProto.Tile.GeomType.UNKNOWN, 0, (f, l) -> {
    throw new IllegalArgumentException("");
  }, "unknown"),
  @JsonProperty("point")
  POINT(VectorTileProto.Tile.GeomType.POINT, 1, FeatureCollector::point, "point"),
  @JsonProperty("line")
  LINE(VectorTileProto.Tile.GeomType.LINESTRING, 2, FeatureCollector::line, "linestring"),
  @JsonProperty("polygon")
  POLYGON(VectorTileProto.Tile.GeomType.POLYGON, 4, FeatureCollector::polygon, "polygon");

  private final VectorTileProto.Tile.GeomType protobufType;
  private final int minPoints;
  private final BiFunction<FeatureCollector, String, Feature> geometryFactory;
  private final String matchTypeString;

  GeometryType(VectorTileProto.Tile.GeomType protobufType, int minPoints,
    BiFunction<FeatureCollector, String, Feature> geometryFactory, String matchTypeString) {
    this.protobufType = protobufType;
    this.minPoints = minPoints;
    this.geometryFactory = geometryFactory;
    this.matchTypeString = matchTypeString;
  }

  public static GeometryType valueOf(Geometry geom) {
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
   * Generates a factory method which creates a {@link Feature} from a {@link FeatureCollector} of the appropriate
   * geometry type.
   *
   * @param layerName - name of the layer
   * @return geometry factory method
   */
  public Function<FeatureCollector, Feature> geometryFactory(String layerName) {
    return features -> geometryFactory.apply(features, layerName);
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
