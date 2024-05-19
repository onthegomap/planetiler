package com.onthegomap.planetiler.reader.parquet;

import blue.strategic.parquet.ParquetReader;
import com.google.common.collect.Iterators;
import com.onthegomap.planetiler.config.Bounds;
import com.onthegomap.planetiler.geo.GeoUtils;
import com.onthegomap.planetiler.geo.GeometryException;
import com.onthegomap.planetiler.reader.SourceFeature;
import com.onthegomap.planetiler.reader.WithTags;
import com.onthegomap.planetiler.util.FunctionThatThrows;
import com.onthegomap.planetiler.util.Hashing;
import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.ToLongFunction;
import java.util.stream.IntStream;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.filter2.predicate.FilterApi;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.filter2.predicate.Filters;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Reads {@link SourceFeature SourceFeatures} from a single
 * <a href="https://github.com/opengeospatial/geoparquet/blob/main/format-specs/geoparquet.md">geoparquet</a> file.
 */
public class ParquetInputFile {

  private static final Logger LOGGER = LoggerFactory.getLogger(ParquetInputFile.class);
  private final ParquetMetadata metadata;
  private final InputFile inputFile;
  private final Path path;
  private final FilterCompat.Filter filter;
  private final String source;
  private final String geometryColumn;
  private final Map<String, FunctionThatThrows<Object, Geometry>> converters = new HashMap<>();
  private final ToLongFunction<Map<String, Object>> idGenerator;
  private final String layer;
  private final long count;
  private final int blockCount;
  private Envelope postFilterBounds = null;
  private boolean outOfBounds = false;
  private final Map<String, Object> extraFields;

  public ParquetInputFile(String source, String layer, Path path) {
    this(source, layer, path, null, Bounds.WORLD, null, null);
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

  public ParquetInputFile(String source, String layer, Path path, FilterPredicate filter, Bounds bounds,
    Map<String, Object> extraFields, Function<Map<String, Object>, Object> idGenerator) {
    this.idGenerator = idGenerator == null ? null : map -> hashToLong(idGenerator.apply(map));
    this.layer = layer;
    this.source = source;
    this.path = path;
    inputFile = ParquetReader.makeInputFile(path.toFile());
    this.extraFields = extraFields;
    try (var file = open()) {
      metadata = file.getFooter();
      var fileMetadata = metadata.getFileMetaData();
      var geoparquet = GeoParquetMetadata.parse(fileMetadata);
      geometryColumn = geoparquet.primaryColumn();
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

        if (columnInfo.crs() != null) {
          // TODO handle projjson
          LOGGER.warn("Custom CRS not supported in {}", path);
        }

        if (column.equals(geometryColumn)) {
          if (columnInfo.bbox() != null && columnInfo.bbox().size() == 4) {
            var bbox = columnInfo.bbox();
            Envelope env = new Envelope(bbox.get(0), bbox.get(2), bbox.get(1), bbox.get(3));
            // TODO apply projection
            if (!bounds.latLon().intersects(env)) {
              this.outOfBounds = true;
            }
          }
          if (!this.outOfBounds && !bounds.isWorld()) {
            var covering = columnInfo.covering();
            // if covering metadata missing, use default bbox:{xmin,xmax,ymin,ymax}
            if (covering == null) {
              var root = metadata.getFileMetaData().getSchema();
              if (hasNumericField(root, "bbox.xmin") &&
                hasNumericField(root, "bbox.xmax") &&
                hasNumericField(root, "bbox.ymin") &&
                hasNumericField(root, "bbox.ymax")) {
                covering = new GeoParquetMetadata.Covering(new GeoParquetMetadata.CoveringBbox(
                  List.of("bbox.xmin"),
                  List.of("bbox.ymin"),
                  List.of("bbox.xmax"),
                  List.of("bbox.ymax")
                ));
              } else if (hasNumericField(root, "bbox", "xmin") &&
                hasNumericField(root, "bbox", "xmax") &&
                hasNumericField(root, "bbox", "ymin") &&
                hasNumericField(root, "bbox", "ymax")) {
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
                fileMetadata.getSchema().getColumnDescription(coveringBbox.xmax().toArray(String[]::new))
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
              var bboxFilter = FilterApi.and(
                FilterApi.and(
                  gtEq.apply(coveringBbox.xmax(), latLonBounds.getMinX()),
                  ltEq.apply(coveringBbox.xmin(), latLonBounds.getMaxX())
                ),
                FilterApi.and(
                  gtEq.apply(coveringBbox.ymax(), latLonBounds.getMinY()),
                  ltEq.apply(coveringBbox.ymin(), latLonBounds.getMaxY())
                )
              );
              filter = filter == null ? bboxFilter : FilterApi.and(filter, bboxFilter);
            } else {
              LOGGER.warn("No covering column specified in geoparquet metadata, fall back to post-filtering");
              postFilterBounds = bounds.latLon();
            }
          }
        }
      }
      count = outOfBounds ? 0 : file.getFilteredRecordCount();
      blockCount = outOfBounds ? 0 : metadata.getBlocks().size();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    this.filter = filter == null ? FilterCompat.NOOP : FilterCompat.get(filter);
  }

  private static long hashToLong(Object o) {
    return switch (o) {
      case String s -> Hashing.fnv1a64(s.getBytes(StandardCharsets.UTF_8));
      case byte[] bs -> Hashing.fnv1a64(bs);
      case Integer i -> i;
      case Long l -> l;
      case Float f -> Float.floatToIntBits(f);
      case Double d -> Double.doubleToLongBits(d);
      case null -> 0;
      default -> Hashing.fnv1a64(o.toString().getBytes(StandardCharsets.UTF_8));
    };
  }

  public boolean hasFilter() {
    return FilterCompat.isFilteringRequired(filter);
  }

  public interface BlockReader extends Iterable<Block>, Closeable {

    @Override
    default void close() throws IOException {}
  }

  public interface Block extends Iterable<ParquetFeature> {

    Path getFileName();

    String layer();
  }

  public BlockReader get() {
    if (outOfBounds) {
      return Collections::emptyIterator;
    }
    long fileHash = Hashing.fnv1a64(path.toString().getBytes(StandardCharsets.UTF_8));
    var schema = metadata.getFileMetaData().getSchema();
    var columnIOFactory = new ColumnIOFactory(metadata.getFileMetaData().getCreatedBy(), false);
    return () -> IntStream.range(0, metadata.getBlocks().size()).mapToObj(blockIndex -> {
      long blockHash = Hashing.fnv1a64(fileHash, ByteBuffer.allocate(4).putInt(blockIndex).array());
      // happens in reader thread
      // TODO read smaller set of rows to reduce memory usage
      return (Block) new Block() {
        @Override
        public Path getFileName() {
          return path;
        }

        @Override
        public String layer() {
          return layer;
        }

        @Override
        public Iterator<ParquetFeature> iterator() {
          PageReadStore group;
          try (var reader = open()) {
            group = reader.readFilteredRowGroup(blockIndex);
            if (group == null) {
              return Collections.emptyIterator();
            }
          } catch (IOException e) {
            throw new UncheckedIOException(e);
          }
          MessageColumnIO columnIO = columnIOFactory.getColumnIO(schema);
          var recordReader = columnIO.getRecordReader(group, new MapRecordMaterializer(schema), filter);
          long total = group.getRowCount();
          return Iterators.filter(new Iterator<>() {
            long i = 0;

            @Override
            public boolean hasNext() {
              return i < total;
            }

            @Override
            public ParquetFeature next() {
              if (!hasNext()) {
                throw new NoSuchElementException();
              }
              i++;

              var item = recordReader.read();

              if (item == null) {
                return null;
              }

              if (extraFields != null) {
                item.putAll(extraFields);
              }

              var feature = new ParquetFeature(
                source,
                layer,
                path,
                idGenerator != null ? idGenerator.applyAsLong(item) :
                  Hashing.fnv1a64(blockHash, ByteBuffer.allocate(8).putLong(i).array()),
                ParquetInputFile.this::readPrimaryGeometry,
                item
              );

              if (postFilterBounds != null) {
                try {
                  if (!feature.latLonGeometry().getEnvelopeInternal().intersects(postFilterBounds)) {
                    return null;
                  }
                } catch (GeometryException e) {
                  LOGGER.warn("Error reading geometry to post-filter bounds", e);
                  return null;
                }
              }

              return feature;
            }
          }, Objects::nonNull);
        }
      };
    }).iterator();
  }

  private ParquetFileReader open() throws IOException {
    return ParquetFileReader.open(inputFile, ParquetReadOptions.builder()
      .withRecordFilter(filter)
      .build());
  }

  private Geometry readPrimaryGeometry(WithTags tags) {
    return readGeometry(tags, geometryColumn);
  }

  private Geometry readGeometry(WithTags tags, String column) {
    var value = tags.getTag(column);
    var converter = converters.get(column);
    if (value == null) {
      LOGGER.warn("Missing {} column", column);
      return GeoUtils.EMPTY_GEOMETRY;
    } else if (converter == null) {
      throw new IllegalArgumentException("No geometry converter for " + column);
    }
    try {
      return converter.apply(value);
    } catch (Exception e) {
      LOGGER.warn("Error reading geometry {}", column, e);
      return GeoUtils.EMPTY_GEOMETRY;
    }
  }

  public long getCount() {
    return count;
  }

  public long getBlockCount() {
    return blockCount;
  }

}
