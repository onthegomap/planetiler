package com.onthegomap.planetiler.reader.parquet;

import com.onthegomap.planetiler.geo.GeoUtils;
import com.onthegomap.planetiler.geo.GeometryException;
import com.onthegomap.planetiler.geo.GeometryType;
import com.onthegomap.planetiler.reader.WithTags;
import com.onthegomap.planetiler.util.FunctionThatThrows;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import org.locationtech.jts.geom.Geometry;

/**
 * Decodes geometries from a parquet record based on the {@link GeoParquetMetadata} provided.
 */
class GeometryReader {

  private final Map<String, FormatHandler> converters = new HashMap<>();
  final String geometryColumn;

  private record FormatHandler(
    FunctionThatThrows<Object, Geometry> parse,
    Function<Object, GeometryType> sniffType
  ) {}

  private static <T> FormatHandler arrowHandler(GeometryType type,
    FunctionThatThrows<T, Geometry> parser) {
    return new FormatHandler(obj -> parser.apply((T) obj), any -> type);
  }

  GeometryReader(GeoParquetMetadata geoparquet) {
    this.geometryColumn = geoparquet.primaryColumn();
    for (var entry : geoparquet.columns().entrySet()) {
      String column = entry.getKey();
      GeoParquetMetadata.ColumnMetadata columnInfo = entry.getValue();
      FormatHandler converter = switch (columnInfo.encoding()) {
        case "WKB" -> new FormatHandler(
          obj -> obj instanceof byte[] bytes ? GeoUtils.wkbReader().read(bytes) : null,
          obj -> obj instanceof byte[] bytes ? GeometryType.fromWKB(bytes) : GeometryType.UNKNOWN
        );
        case "WKT" -> new FormatHandler(
          obj -> obj instanceof String string ? GeoUtils.wktReader().read(string) : null,
          obj -> obj instanceof String string ? GeometryType.fromWKT(string) : GeometryType.UNKNOWN
        );
        case "multipolygon" ->
          arrowHandler(GeometryType.POLYGON, GeoArrow::multipolygon);
        case "polygon" ->
          arrowHandler(GeometryType.POLYGON, GeoArrow::polygon);
        case "multilinestring" ->
          arrowHandler(GeometryType.LINE, GeoArrow::multilinestring);
        case "linestring" ->
          arrowHandler(GeometryType.LINE, GeoArrow::linestring);
        case "multipoint" ->
          arrowHandler(GeometryType.POINT, GeoArrow::multipoint);
        case "point" ->
          arrowHandler(GeometryType.POINT, GeoArrow::point);
        default -> throw new IllegalArgumentException("Unhandled type: " + columnInfo.encoding());
      };
      converters.put(column, converter);
    }
  }

  Geometry readPrimaryGeometry(WithTags tags) throws GeometryException {
    return readGeometry(tags, geometryColumn);
  }

  Geometry readGeometry(WithTags tags, String column) throws GeometryException {
    return parseGeometry(tags.getTag(column), column);
  }

  Geometry parseGeometry(Object value, String column) throws GeometryException {
    var converter = converters.get(column);
    if (value == null) {
      throw new GeometryException("no_parquet_column", "Missing geometry column column " + column);
    } else if (converter == null) {
      throw new GeometryException("no_converter", "No geometry converter for " + column);
    }
    try {
      return converter.parse.apply(value);
    } catch (Exception e) {
      throw new GeometryException("error_reading", "Error reading " + column, e);
    }
  }

  GeometryType sniffGeometryType(Object value, String column) {
    var converter = converters.get(column);
    if (value != null && converter != null) {
      return converter.sniffType.apply(value);
    }
    return GeometryType.UNKNOWN;
  }
}
