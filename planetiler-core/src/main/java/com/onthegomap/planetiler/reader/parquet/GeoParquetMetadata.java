package com.onthegomap.planetiler.reader.parquet;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.parquet.hadoop.metadata.FileMetaData;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Struct for deserializing a
 * <a href="https://github.com/opengeospatial/geoparquet/blob/main/format-specs/geoparquet.md#file-metadata">geoparquet
 * metadata</a> json string into.
 */
@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
public record GeoParquetMetadata(
  String version,
  String primaryColumn,
  Map<String, ColumnMetadata> columns
) {

  private static final Logger LOGGER = LoggerFactory.getLogger(GeoParquetMetadata.class);

  private static final ObjectMapper mapper =
    new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

  @JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
  public record ColumnMetadata(
    String encoding,
    List<String> geometryTypes,
    Object crs,
    String orientation,
    String edges,
    List<Double> bbox,
    Double epoch,
    Covering covering
  ) {
    ColumnMetadata(String encoding) {
      this(encoding, List.of());
    }

    ColumnMetadata(String encoding, List<String> geometryTypes) {
      this(encoding, geometryTypes, null, null, null, null, null, null);
    }
  }

  public record CoveringBbox(
    List<String> xmin,
    List<String> ymin,
    List<String> xmax,
    List<String> ymax
  ) {}

  public record Covering(
    CoveringBbox bbox
  ) {}


  public static GeoParquetMetadata parse(FileMetaData metadata) throws IOException {
    String string = metadata.getKeyValueMetaData().get("geo");
    if (string != null) {
      try {
        return mapper.readValue(string, GeoParquetMetadata.class);
      } catch (JsonProcessingException e) {
        LOGGER.warn("Invalid geoparquet metadata", e);
      }
    }
    // fallback
    for (var field : metadata.getSchema().asGroupType().getFields()) {
      if (field.isPrimitive() &&
        field.asPrimitiveType().getPrimitiveTypeName() == PrimitiveType.PrimitiveTypeName.BINARY) {
        switch (field.getName()) {
          case "geometry", "wkb_geometry" -> {
            return new GeoParquetMetadata("1.0.0", field.getName(), Map.of(
              field.getName(), new ColumnMetadata("WKB")));
          }
          case "wkt_geometry" -> {
            return new GeoParquetMetadata("1.0.0", field.getName(), Map.of(
              field.getName(), new ColumnMetadata("WKT")));
          }
          default -> {
            //ignore
          }
        }
      }
    }
    throw new IOException(
      "No valid geometry columns found: " + metadata.getSchema().asGroupType().getFields().stream().map(
        Type::getName).toList());
  }
}
