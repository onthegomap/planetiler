package com.onthegomap.planetiler.reader.parquet;

import com.google.common.collect.Iterators;
import com.onthegomap.planetiler.Profile;
import com.onthegomap.planetiler.config.Bounds;
import com.onthegomap.planetiler.expression.Expression;
import com.onthegomap.planetiler.geo.GeometryException;
import com.onthegomap.planetiler.geo.GeometryType;
import com.onthegomap.planetiler.reader.SourceFeature;
import com.onthegomap.planetiler.reader.remote.RangeFile;
import com.onthegomap.planetiler.util.Hashing;
import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.function.ToLongFunction;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.filter2.predicate.FilterApi;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.filter2.predicate.SchemaCompatibilityValidator;
import org.apache.parquet.filter2.statisticslevel.StatisticsFilter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.MessageColumnIO;
import org.locationtech.jts.geom.Envelope;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Reads {@link SourceFeature SourceFeatures} from a single
 * <a href="https://github.com/opengeospatial/geoparquet/blob/main/format-specs/geoparquet.md">geoparquet</a> file.
 * <p>
 * Close it when done to release the file.
 */
public class ParquetInputFile implements Closeable {

  private static final Logger LOGGER = LoggerFactory.getLogger(ParquetInputFile.class);
  // the largest byte array the JVM can allocate
  private static final long MAX_FETCH_BYTES = Integer.MAX_VALUE - 8L;
  private final ParquetMetadata metadata;
  private final RangeFile file;
  private final Path path;
  private final FilterCompat.Filter filter;
  private final FilterPredicate predicate;
  private final String source;
  private final ToLongFunction<Map<String, Object>> idGenerator;
  private final String layer;
  private final long count;
  private final List<Integer> rowGroupsToRead;
  final GeometryReader geometryReader;
  private final Map<String, Object> extraFields;
  private final Set<GeometryType> geometryTypes;
  private final GeoParquetMetadata geoparquet;
  private Envelope postFilterBounds = null;
  private boolean outOfBounds = false;

  public ParquetInputFile(String source, String layer, Path path) {
    this(source, layer, path, null, Bounds.WORLD, null, null);
  }

  public ParquetInputFile(String source, String layer, Path path, FilterPredicate filter, Bounds bounds,
    Map<String, Object> extraFields, Function<Map<String, Object>, Object> idGenerator) {
    this(source, layer, path, openLocal(path), filter, bounds, extraFields, idGenerator);
  }

  private static RangeFile openLocal(Path path) {
    try {
      return RangeFile.openLocal(path);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /**
   * Reads from {@code file}, which may be remote.
   * <p>
   * {@code path} is where the file would be on local disk: it identifies the file in features and logs, and feature IDs
   * are derived from it when there is no {@code idGenerator}, so pass the same path whether {@code file} is local or
   * remote to get the same output.
   */
  public ParquetInputFile(String source, String layer, Path path, RangeFile file, FilterPredicate filter,
    Bounds bounds, Map<String, Object> extraFields, Function<Map<String, Object>, Object> idGenerator) {
    this.idGenerator = idGenerator == null ? null : map -> hashToLong(idGenerator.apply(map));
    this.layer = layer;
    this.source = source;
    this.path = path;
    this.file = file;
    this.extraFields = extraFields;
    try (var reader = ParquetFileReader.open(new RangeInputFile(file, file.length()))) {
      metadata = reader.getFooter();
      geoparquet = GeoParquetMetadata.parse(metadata.getFileMetaData());
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    var fileMetadata = metadata.getFileMetaData();
    this.geometryReader = new GeometryReader(geoparquet);
    this.geometryTypes = geoparquet.geometryTypes();
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
    if (filter != null) {
      SchemaCompatibilityValidator.validate(filter, fileMetadata.getSchema());
    }
    this.predicate = filter;
    this.filter = filter == null ? FilterCompat.NOOP : FilterCompat.get(filter);
    var blocks = metadata.getBlocks();
    rowGroupsToRead = outOfBounds ? List.of() : IntStream.range(0, blocks.size())
      .filter(i -> mayMatch(blocks.get(i)) && mayIntersect(blocks.get(i), bounds))
      .boxed()
      .toList();
    count = rowGroupsToRead.stream().mapToLong(i -> blocks.get(i).getRowCount()).sum();
  }

  private boolean mayMatch(BlockMetaData rowGroup) {
    // Only use row group stats from filter, the embedded indexes would require many range requests which slow down
    // remote reads
    return predicate == null || !StatisticsFilter.canDrop(predicate, rowGroup.getColumns());
  }

  private boolean mayIntersect(BlockMetaData rowGroup, Bounds bounds) {
    if (bounds.isWorld()) {
      return true;
    }
    // parquet-java's own row group filter ignores floating point column statistics unless the file records how many
    // NaNs each column has, which most writers (including Overture's) omit, so check the bbox statistics here. Rows
    // with a NaN bbox can't be in bounds anyway.
    var envelope =
      geoparquet.primaryColumnMetadata().rowGroupEnvelope(metadata.getFileMetaData().getSchema(), rowGroup);
    return envelope == null || envelope.intersects(bounds.latLon());
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

  public boolean shouldSkip(Profile profile) {
    return outOfBounds ||
      !profile.caresAbout(new Expression.PartialInput(Set.of(source), Set.of(layer), extraFields, geometryTypes));
  }

  public BlockReader get() {
    long fileHash = Hashing.fnv1a64(path.toString().getBytes(StandardCharsets.UTF_8));
    var schema = metadata.getFileMetaData().getSchema();
    var columnIOFactory = new ColumnIOFactory(metadata.getFileMetaData().getCreatedBy(), false);
    return () -> rowGroupsToRead.stream().map(blockIndex -> {
      var block = metadata.getBlocks().get(blockIndex);
      long blockHash = Hashing.fnv1a64(fileHash, ByteBuffer.allocate(4).putInt(blockIndex).array());
      var span = rowGroupSpan(block);
      return (Block) new Block() {
        private RangeFile.Pending bytes = null;

        @Override
        public Path getFileName() {
          return path;
        }

        @Override
        public String layer() {
          return layer;
        }

        @Override
        public void startFetchIfNecessary() {
          // larger row groups get read by parquet in smaller pieces instead
          if (span.length() <= MAX_FETCH_BYTES) {
            bytes = file.readLater(span.start(), (int) span.length());
          }
        }

        @Override
        public Iterator<ParquetFeature> iterator() {
          PageReadStore group;
          try {
            byte[] prefetched = bytes == null ? null : bytes.get();
            bytes = null;
            var inputFile = new RangeInputFile(file, file.length(), prefetched, span.start());
            // reuse the footer parsed in the constructor instead of reading it again for each row group
            try (
              var reader = ParquetFileReader.open(inputFile,
                new ParquetMetadata(metadata.getFileMetaData(), List.of(block)), ParquetReadOptions.builder().build(),
                inputFile.newStream())
            ) {
              group = reader.readRowGroup(0);
            }
          } catch (IOException e) {
            throw new UncheckedIOException(e);
          }
          MessageColumnIO columnIO = columnIOFactory.getColumnIO(schema);
          var recordReader = columnIO.getRecordReader(group, new ParquetRecordConverter(schema, geoparquet), filter);
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
                idGenerator != null ? idGenerator.applyAsLong(item) :
                  Hashing.fnv1a64(blockHash, ByteBuffer.allocate(8).putLong(i).array()),
                geometryReader,
                item,
                path,
                schema
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

  public long getCount() {
    return count;
  }

  public long getBlockCount() {
    return rowGroupsToRead.size();
  }

  /** Returns the number of bytes in the row groups that will be read. */
  public long getBytesToRead() {
    return rowGroupsToRead.stream().mapToLong(i -> rowGroupSpan(metadata.getBlocks().get(i)).length()).sum();
  }

  /** Returns how many bytes reading the row groups will fetch from a remote server: 0 if they are all local. */
  public long getBytesToFetch() {
    return rowGroupSpans().mapToLong(span -> file.bytesToFetch(span.start(), Math.toIntExact(span.length()))).sum();
  }

  /** Returns how many requests reading the row groups will make to a remote server: 0 if they are all local. */
  public long getRequestsToFetch() {
    return rowGroupSpans().mapToLong(span -> file.requestsToFetch(span.start(), Math.toIntExact(span.length()))).sum();
  }

  // row groups too large for one array get read by parquet in pieces, which these estimates don't cover
  private Stream<Span> rowGroupSpans() {
    return rowGroupsToRead.stream()
      .map(i -> rowGroupSpan(metadata.getBlocks().get(i)))
      .filter(span -> span.length() <= MAX_FETCH_BYTES);
  }

  /** Returns the file this reads from, to report how much of it got fetched from a remote server. */
  public RangeFile file() {
    return file;
  }

  @Override
  public void close() throws IOException {
    file.close();
  }

  private record Span(long start, long end) {
    long length() {
      return end - start;
    }
  }

  // the column chunks of a row group are next to each other, so one read gets all of them
  private static Span rowGroupSpan(BlockMetaData block) {
    long start = block.getColumns().stream().mapToLong(ColumnChunkMetaData::getStartingPos).min().orElse(0);
    long end = block.getColumns().stream().mapToLong(c -> c.getStartingPos() + c.getTotalSize()).max().orElse(0);
    return new Span(start, end);
  }

  public interface BlockReader extends Iterable<Block>, Closeable {

    @Override
    default void close() throws IOException {}
  }

  public interface Block extends Iterable<ParquetFeature> {

    Path getFileName();

    String layer();

    /**
     * Starts fetching this block's bytes in the background if the file is remote, so that iterating over it later
     * blocks less. Returns right away, and does nothing for local files.
     */
    default void startFetchIfNecessary() {}
  }

}
