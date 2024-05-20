package com.onthegomap.planetiler.reader.parquet;

import blue.strategic.parquet.ParquetReader;
import com.google.common.collect.Iterators;
import com.onthegomap.planetiler.config.Bounds;
import com.onthegomap.planetiler.geo.GeometryException;
import com.onthegomap.planetiler.reader.SourceFeature;
import com.onthegomap.planetiler.util.Hashing;
import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.ToLongFunction;
import java.util.stream.IntStream;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.filter2.predicate.FilterApi;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.MessageColumnIO;
import org.locationtech.jts.geom.Envelope;
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
  private final ToLongFunction<Map<String, Object>> idGenerator;
  private final String layer;
  private final long count;
  private final int blockCount;
  private final GeometryReader geometryReader;
  private Envelope postFilterBounds = null;
  private boolean outOfBounds = false;
  private final Map<String, Object> extraFields;

  public ParquetInputFile(String source, String layer, Path path) {
    this(source, layer, path, null, Bounds.WORLD, null, null);
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
      this.geometryReader = new GeometryReader(geoparquet);
      if (!bounds.isWorld()) {
        if (!geoparquet.primaryColumnMetadata().envelope().intersects(bounds.latLon())) {
          outOfBounds = true;
        } else {
          var bboxFilter = geoparquet.primaryColumnMetadata().bboxFilter(fileMetadata.getSchema(), bounds);
          if (bboxFilter != null) {
            filter = filter == null ? bboxFilter : FilterApi.and(filter, bboxFilter);
          } else {
            LOGGER.warn("No covering column specified in geoparquet metadata, fall back to post-filtering");
            postFilterBounds = bounds.latLon();
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
          var recordReader = columnIO.getRecordReader(group, new ParquetRecordConverter(schema), filter);
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
                geometryReader::readPrimaryGeometry,
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

  public long getCount() {
    return count;
  }

  public long getBlockCount() {
    return blockCount;
  }

}
