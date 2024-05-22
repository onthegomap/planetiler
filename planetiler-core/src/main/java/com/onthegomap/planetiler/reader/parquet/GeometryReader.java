package com.onthegomap.planetiler.reader.parquet;

import com.onthegomap.planetiler.geo.GeoUtils;
import com.onthegomap.planetiler.geo.GeometryException;
import com.onthegomap.planetiler.reader.WithTags;
import com.onthegomap.planetiler.util.FunctionThatThrows;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.locationtech.jts.geom.Geometry;

/**
 * Decodes geometries from a parquet record based on the {@link GeoParquetMetadata} provided.
 */
class GeometryReader {
  private final Map<String, FunctionThatThrows<Object, Geometry>> converters = new HashMap<>();
  private final String geometryColumn;

  GeometryReader(GeoParquetMetadata geoparquet) {
    this.geometryColumn = geoparquet.primaryColumn();
    for (var entry : geoparquet.columns().entrySet()) {
      String column = entry.getKey();
      GeoParquetMetadata.ColumnMetadata columnInfo = entry.getValue();
      FunctionThatThrows<Object, Geometry> converter = switch (columnInfo.encoding()) {
        case "WKB" -> obj -> obj instanceof byte[] bytes ? GeoUtils.wkbReader().read(bytes) : null;
        case "WKT" -> obj -> obj instanceof String string ? GeoUtils.wktReader().read(string) : null;
        case "multipolygon", "geoarrow.multipolygon" ->
          obj -> obj instanceof List<?> list ? GeoArrow.multipolygon((List<List<List<Object>>>) list) : null;
        case "polygon", "geoarrow.polygon" ->
          obj -> obj instanceof List<?> list ? GeoArrow.polygon((List<List<Object>>) list) : null;
        case "multilinestring", "geoarrow.multilinestring" ->
          obj -> obj instanceof List<?> list ? GeoArrow.multilinestring((List<List<Object>>) list) : null;
        case "linestring", "geoarrow.linestring" ->
          obj -> obj instanceof List<?> list ? GeoArrow.linestring((List<Object>) list) : null;
        case "multipoint", "geoarrow.multipoint" ->
          obj -> obj instanceof List<?> list ? GeoArrow.multipoint((List<Object>) list) : null;
        case "point", "geoarrow.point" -> GeoArrow::point;
        default -> throw new IllegalArgumentException("Unhandled type: " + columnInfo.encoding());
      };

      converters.put(column, converter);
    }
  }

  Geometry readPrimaryGeometry(WithTags tags) throws GeometryException {
    return readGeometry(tags, geometryColumn);
  }

  Geometry readGeometry(WithTags tags, String column) throws GeometryException {
    var value = tags.getTag(column);
    var converter = converters.get(column);
    if (value == null) {
      throw new GeometryException("no_parquet_column", "Missing geometry column column " + column);
    } else if (converter == null) {
      throw new GeometryException("no_converter", "No geometry converter for " + column);
    }
    try {
      return converter.apply(value);
    } catch (Exception e) {
      throw new GeometryException("error_reading", "Error reading " + column, e);
    }
  }
}
