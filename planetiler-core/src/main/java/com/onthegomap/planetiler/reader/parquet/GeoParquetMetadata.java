package com.onthegomap.planetiler.reader.parquet;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.onthegomap.planetiler.config.Bounds;
import com.onthegomap.planetiler.geo.GeoUtils;
import com.onthegomap.planetiler.geo.GeometryType;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiFunction;
import org.apache.parquet.filter2.predicate.FilterApi;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.filter2.predicate.Filters;
import org.apache.parquet.hadoop.metadata.FileMetaData;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.locationtech.jts.geom.Envelope;
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

  /**
   * Returns the {@link GeometryType GeometryTypes} that can be contained in this geoparquet file or an empty set if
   * unknown.
   * <p>
   * These can come from geoarrow encoding type, or the {@code geometry_types} attributes.
   */
  public Set<GeometryType> geometryTypes() {
    Set<GeometryType> types = new HashSet<>();
    for (var type : primaryColumnMetadata().geometryTypes()) {
      types.add(switch (type) {
        case "Point", "MultiPoint" -> GeometryType.POINT;
        case "LineString", "MultiLineString" -> GeometryType.LINE;
        case "Polygon", "MultiPolygon" -> GeometryType.POLYGON;
        case null, default -> GeometryType.UNKNOWN;
      });
    }
    // geoarrow
    String encoding = primaryColumnMetadata().encoding();
    if (encoding.contains("polygon")) {
      types.add(GeometryType.POLYGON);
    } else if (encoding.contains("point")) {
      types.add(GeometryType.POINT);
    } else if (encoding.contains("linestring")) {
      types.add(GeometryType.LINE);
    }
    return types;
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

    public Envelope envelope() {
      return (bbox == null || bbox.size() != 4) ? GeoUtils.WORLD_LAT_LON_BOUNDS :
        new Envelope(bbox.get(0), bbox.get(2), bbox.get(1), bbox.get(3));
    }

    /**
     * Returns a parquet filter that filters records read to only those where the covering bbox overlaps {@code bounds}
     * or null if unable to infer that from the metadata.
     * <p>
     * If covering bbox metadata is missing from geoparquet metadata, it will try to use bbox.xmin, bbox.xmax,
     * bbox.ymin, and bbox.ymax if present.
     */
    public FilterPredicate bboxFilter(MessageType schema, Bounds bounds) {
      if (!bounds.isWorld()) {
        var covering = covering();
        // if covering metadata missing, use default bbox:{xmin,xmax,ymin,ymax}
        if (covering == null) {
          if (hasNumericField(schema, "bbox.xmin") &&
            hasNumericField(schema, "bbox.xmax") &&
            hasNumericField(schema, "bbox.ymin") &&
            hasNumericField(schema, "bbox.ymax")) {
            covering = new GeoParquetMetadata.Covering(new GeoParquetMetadata.CoveringBbox(
              List.of("bbox.xmin"),
              List.of("bbox.ymin"),
              List.of("bbox.xmax"),
              List.of("bbox.ymax")
            ));
          } else if (hasNumericField(schema, "bbox", "xmin") &&
            hasNumericField(schema, "bbox", "xmax") &&
            hasNumericField(schema, "bbox", "ymin") &&
            hasNumericField(schema, "bbox", "ymax")) {
            covering = new GeoParquetMetadata.Covering(new GeoParquetMetadata.CoveringBbox(
              List.of("bbox", "xmin"),
              List.of("bbox", "ymin"),
              List.of("bbox", "xmax"),
              List.of("bbox", "ymax")
            ));
          }
        }
        if (covering != null) {
          var latLonBounds = bounds.latLon();
          // TODO apply projection
          var coveringBbox = covering.bbox();
          var coordinateType =
            schema.getColumnDescription(coveringBbox.xmax().toArray(String[]::new))
              .getPrimitiveType()
              .getPrimitiveTypeName();
          BiFunction<List<String>, Number, FilterPredicate> gtEq = switch (coordinateType) {
            case DOUBLE -> (p, v) -> FilterApi.gtEq(Filters.doubleColumn(p), v.doubleValue());
            case FLOAT -> (p, v) -> FilterApi.gtEq(Filters.floatColumn(p), v.floatValue());
            default -> throw new UnsupportedOperationException();
          };
          BiFunction<List<String>, Number, FilterPredicate> ltEq = switch (coordinateType) {
            case DOUBLE -> (p, v) -> FilterApi.ltEq(Filters.doubleColumn(p), v.doubleValue());
            case FLOAT -> (p, v) -> FilterApi.ltEq(Filters.floatColumn(p), v.floatValue());
            default -> throw new UnsupportedOperationException();
          };
          return FilterApi.and(
            FilterApi.and(
              gtEq.apply(coveringBbox.xmax(), latLonBounds.getMinX()),
              ltEq.apply(coveringBbox.xmin(), latLonBounds.getMaxX())
            ),
            FilterApi.and(
              gtEq.apply(coveringBbox.ymax(), latLonBounds.getMinY()),
              ltEq.apply(coveringBbox.ymin(), latLonBounds.getMaxY())
            )
          );
        }
      }
      return null;
    }

    /** Returns the geoarrow type string of this geometry column, or null if not geoarrow. */
    public String getGeoArrowType() {
      return (encoding != null && (encoding.contains("polygon") ||
        encoding.contains("point") ||
        encoding.contains("line"))) ? encoding : null;
    }
  }

  public ColumnMetadata primaryColumnMetadata() {
    return Objects.requireNonNull(columns.get(primaryColumn),
      "No geoparquet metadata for primary column " + primaryColumn);
  }


  /**
   * Extracts geoparquet metadata from the {@code "geo"} key value metadata field for the file, or tries to generate a
   * default one if missing that uses geometry, wkb_geometry, or wkt_geometry column.
   */
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

  private static boolean hasNumericField(MessageType root, String... path) {
    if (root.containsPath(path)) {
      var type = root.getType(path);
      if (!type.isPrimitive()) {
        return false;
      }
      var typeName = type.asPrimitiveType().getPrimitiveTypeName();
      return typeName == PrimitiveType.PrimitiveTypeName.DOUBLE || typeName == PrimitiveType.PrimitiveTypeName.FLOAT;
    }
    return false;
  }
}
