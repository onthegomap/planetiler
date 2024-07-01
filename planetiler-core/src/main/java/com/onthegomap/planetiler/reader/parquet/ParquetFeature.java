package com.onthegomap.planetiler.reader.parquet;

import com.onthegomap.planetiler.geo.GeoUtils;
import com.onthegomap.planetiler.geo.GeometryException;
import com.onthegomap.planetiler.geo.GeometryType;
import com.onthegomap.planetiler.reader.SourceFeature;
import com.onthegomap.planetiler.reader.Struct;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.apache.parquet.schema.MessageType;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Lineal;
import org.locationtech.jts.geom.Polygonal;
import org.locationtech.jts.geom.Puntal;

/**
 * A single record read from a geoparquet file.
 */
public class ParquetFeature extends SourceFeature {

  private final GeometryReader geometryParser;
  private final Object rawGeometry;
  private final Path path;
  private final MessageType schema;
  private Geometry latLon;
  private Geometry world;
  private Struct struct = null;
  private GeometryType geometryType = null;

  ParquetFeature(String source, String sourceLayer, long id, GeometryReader geometryParser,
    Map<String, Object> tags, Path path, MessageType schema) {
    super(tags, source, sourceLayer, List.of(), id);
    this.geometryParser = geometryParser;
    this.rawGeometry = tags.remove(geometryParser.geometryColumn);
    this.path = path;
    this.schema = schema;
  }

  /** Returns the parquet file that this feature was read from. */
  public Path path() {
    return path;
  }

  /** Returns the {@link MessageType} schema of the parquet file that this feature was read from. */
  public MessageType parquetSchema() {
    return schema;
  }

  // todo linespace
  public String geometryColumn() {
    return geometryParser.geometryColumn;
  }

  @Override
  public Geometry latLonGeometry() throws GeometryException {
    return latLon == null ? latLon = geometryParser.parseGeometry(rawGeometry, geometryParser.geometryColumn) : latLon;
  }

  @Override
  public Geometry worldGeometry() throws GeometryException {
    return world != null ? world :
      (world = GeoUtils.sortPolygonsByAreaDescending(GeoUtils.latLonToWorldCoords(latLonGeometry())));
  }

  // todo linespace
  public GeometryType geometryType() {
    if (geometryType != null) {
      return geometryType;
    }
    geometryType = geometryParser.sniffGeometryType(rawGeometry, geometryParser.geometryColumn);
    if (geometryType == GeometryType.UNKNOWN) {
      try {
        geometryType = switch (latLonGeometry()) {
          case Puntal ignored -> GeometryType.POINT;
          case Lineal ignored -> GeometryType.LINE;
          case Polygonal ignored -> GeometryType.POLYGON;
          default -> GeometryType.UNKNOWN;
        };
      } catch (GeometryException e) {
        throw new IllegalStateException(e);
      }
    }
    return geometryType;
  }

  @Override
  public boolean isPoint() {
    return geometryType() == GeometryType.POINT;
  }

  @Override
  public boolean canBePolygon() {
    return geometryType() == GeometryType.POLYGON;
  }

  @Override
  public boolean canBeLine() {
    return geometryType() == GeometryType.LINE;
  }

  private Struct cachedStruct() {
    return struct != null ? struct : (struct = Struct.of(tags()));
  }

  @Override
  public Struct getStruct(String key) {
    return cachedStruct().get(key);
  }

  @Override
  public Struct getStruct(Object key, Object... others) {
    return cachedStruct().get(key, others);
  }

  @Override
  public Object getTag(String key) {
    return cachedStruct().get(key).rawValue();
  }

  @Override
  public Object getTag(String key, Object defaultValue) {
    var value = getTag(key);
    if (value == null) {
      value = defaultValue;
    }
    return value;
  }

  @Override
  public boolean hasTag(String key) {
    return !cachedStruct().get(key).isNull();
  }

  @Override
  public String toString() {
    return tags().toString();
  }
}
