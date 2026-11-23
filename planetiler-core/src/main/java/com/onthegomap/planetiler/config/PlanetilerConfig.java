package com.onthegomap.planetiler.config;

import com.onthegomap.planetiler.archive.TileArchiveConfig;
import com.onthegomap.planetiler.archive.TileCompression;
import com.onthegomap.planetiler.archive.TileFormat;
import com.onthegomap.planetiler.collection.LongLongMap;
import com.onthegomap.planetiler.collection.Storage;
import com.onthegomap.planetiler.reader.osm.PolyFileReader;
import com.onthegomap.planetiler.util.Parse;
import io.soabase.recordbuilder.core.RecordBuilder;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Holder for common parameters used by many components in planetiler.
 */
@RecordBuilder
@RecordBuilder.Options(builderMode = RecordBuilder.BuilderMode.STAGED)
public record PlanetilerConfig(
  Arguments arguments,
  Bounds bounds,
  int threads,
  int featureWriteThreads,
  int featureProcessThreads,
  int featureReadThreads,
  int tileWriteThreads,
  Duration logInterval,
  int minzoom,
  int maxzoom,
  int maxzoomForRendering,
  boolean force,
  boolean append,
  boolean compressTempStorage,
  boolean mmapTempStorage,
  int sortMaxReaders,
  int sortMaxWriters,
  String nodeMapType,
  String nodeMapStorage,
  boolean nodeMapMadvise,
  String multipolygonGeometryStorage,
  boolean multipolygonGeometryMadvise,
  String httpUserAgent,
  Duration httpTimeout,
  int httpRetries,
  Duration httpRetryWait,
  Duration httpRetryMaxWait,
  long downloadChunkSizeMB,
  int downloadThreads,
  double downloadMaxBandwidth,
  int downloadMaxRequests,
  boolean downloadFailIfChanged,
  double minFeatureSizeAtMaxZoom,
  double minFeatureSizeBelowMaxZoom,
  double simplifyToleranceAtMaxZoom,
  double simplifyToleranceBelowMaxZoom,
  boolean osmLazyReads,
  boolean skipFilledTiles,
  int tileWarningSizeBytes,
  Boolean color,
  boolean keepUnzippedSources,
  TileCompression tileCompression,
  TileFormat tileFormat,
  boolean excludeIds,
  boolean mltFsst,
  boolean mltFastPfor,
  boolean mltTessellatePolygons,
  boolean mltPolygonOutline,
  boolean mltReorderFeature,
  boolean mltSharedDictionaries,
  boolean outputLayerStats,
  String debugUrlPattern,
  Path tmpDir,
  Path tileWeights,
  double maxPointBuffer,
  boolean logJtsExceptions,
  int featureSourceIdMultiplier,
  List<String> extraNameTags,
  boolean reuseFeatureDb,
  boolean parallelTempIO
) {

  private static final Logger LOGGER = LoggerFactory.getLogger(PlanetilerConfig.class);

  public static final int MIN_MINZOOM = 0;
  public static final int MAX_MAXZOOM = 16;
  private static final int DEFAULT_MAXZOOM = 14;

  public PlanetilerConfig {
    if (minzoom > maxzoom) {
      throw new IllegalArgumentException("Minzoom cannot be greater than maxzoom");
    }
    if (minzoom < MIN_MINZOOM) {
      throw new IllegalArgumentException("Minzoom must be >= " + MIN_MINZOOM + ", was " + minzoom);
    }
    if (maxzoom > MAX_MAXZOOM) {
      throw new IllegalArgumentException("Max zoom must be <= " + MAX_MAXZOOM + ", was " + maxzoom);
    }
    if (httpRetries < 0) {
      throw new IllegalArgumentException("HTTP Retries must be >= 0, was " + httpRetries);
    }
    if (downloadChunkSizeMB <= 0) {
      throw new IllegalArgumentException("--download-chunk-size-mb must be > 0, was " + downloadChunkSizeMB);
    }
    if (downloadThreads <= 0) {
      throw new IllegalArgumentException("--download-threads must be > 0, was " + downloadThreads);
    }
    if (downloadMaxRequests <= 0) {
      throw new IllegalArgumentException("--download-max-requests must be > 0, was " + downloadMaxRequests);
    }
    if (tileFormat != TileFormat.MLT &&
      (mltFsst || mltFastPfor || mltTessellatePolygons || mltPolygonOutline || mltReorderFeature ||
        mltSharedDictionaries)) {
      throw new IllegalArgumentException("mlt_* options require tile_format=mlt, was " + tileFormat.id());
    }
    // FSST and FastPFOR shrink the uncompressed tile but leave output that gzip cannot compress further
    if (tileFormat == TileFormat.MLT && tileCompression == TileCompression.GZIP) {
      if (mltFsst) {
        LOGGER.warn("mlt_fsst usually makes gzip-compressed tiles larger, use tile_compression=none or disable it");
      }
      if (mltFastPfor) {
        LOGGER.warn("mlt_fastpfor makes gzip-compressed tiles larger, use tile_compression=none or disable it");
      }
    }
  }

  public static PlanetilerConfig defaults() {
    return from(Arguments.of());
  }

  public static PlanetilerConfig from(Arguments arguments) {
    var input = arguments.withGroup(ArgumentGroup.INPUT);
    var output = arguments.withGroup(ArgumentGroup.OUTPUT);
    var outputAdvanced = arguments.withGroup(ArgumentGroup.OUTPUT_ADVANCED);
    var processing = arguments.withGroup(ArgumentGroup.PROCESSING);
    var processingAdvanced = arguments.withGroup(ArgumentGroup.PROCESSING_ADVANCED);
    var logging = arguments.withGroup(ArgumentGroup.LOGGING);
    var downloads = arguments.withGroup(ArgumentGroup.DOWNLOADS);
    var debugging = arguments.withGroup(ArgumentGroup.DEBUGGING);
    // use --madvise and --storage options as default for temp storage, but allow users to override them explicitly for
    // multipolygon geometries or node locations
    boolean defaultMadvise = processingAdvanced.getBoolean(
      "madvise",
      "default value for whether to use linux madvise(random) to improve memory-mapped read performance for temporary storage",
      true);
    // nodemap_storage was previously the only option, so if that's set make it the default
    String fallbackTempStorage = arguments.getArg("nodemap_storage", Storage.MMAP.id());
    String defaultTempStorage = processing.getString(
      "storage",
      "temporary data storage: " + Stream.of(Storage.values()).map(Storage::id)
        .collect(Collectors.joining(", ")),
      fallbackTempStorage);
    boolean parallelTempIO = processingAdvanced.getBoolean(
      "parallel_tmp_io",
      "use unlimited parallelism reading/writing temp files (ie. if using tmpfs or ramfs for temp storage)",
      false);
    int threads = processing.threads();
    int featureWriteThreads = processingAdvanced.getInteger(
      "write_threads",
      "number of threads to use when writing temp features",
      // defaults: <48 cpus=1 writer, 48-80=2 writers, 80-112=3 writers, 112-144=4 writers, ...
      parallelTempIO ? 1 : Math.max(1, (threads - 16) / 32 + 1));
    int featureProcessThreads = processingAdvanced.getInteger(
      "process_threads",
      "number of threads to use when processing input features",
      Math.max(threads < 8 ? threads : (threads - featureWriteThreads), 1));
    Bounds bounds =
      new Bounds(input.bounds("bounds", "only build tiles inside these bounds (default: the input's bounds)"));
    Path polygonFile =
      input.file("polygon", "only build tiles touching the shape in this .poly file", null);
    if (polygonFile != null) {
      try {
        bounds.setShape(PolyFileReader.parsePolyFile(polygonFile));
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }

    int minzoom = output.getInteger("minzoom", "lowest zoom level", MIN_MINZOOM);
    int maxzoom = output.getInteger("maxzoom", "highest zoom level, up to " + MAX_MAXZOOM, DEFAULT_MAXZOOM);
    int renderMaxzoom = outputAdvanced.getInteger(
      "render_maxzoom",
      "maximum rendering zoom level up to " + MAX_MAXZOOM,
      Math.max(maxzoom, DEFAULT_MAXZOOM));
    Path tmpDir = processing.file("tmpdir|tmp", "temporary files, deleted at the end", Path.of("data", "tmp"));
    List<String> extraNameTags = outputAdvanced.getList(
      "extra_name_tags",
      "extra name tags to copy from OSM to output",
      List.of());

    boolean mltAdvanced =
      outputAdvanced.getBoolean("mlt_advanced", "use FSST and FastPFOR encoding schemes when tile format=MLT", false);

    return PlanetilerConfigBuilder.builder()
      .arguments(arguments)
      .bounds(bounds)
      .threads(threads)
      .featureWriteThreads(featureWriteThreads)
      .featureProcessThreads(featureProcessThreads)
      .featureReadThreads(processingAdvanced.getInteger(
        "feature_read_threads",
        "number of threads to use when reading features at tile write time",
        threads < 32 ? 1 : 2))
      .tileWriteThreads(processingAdvanced.getInteger(
        "tile_write_threads",
        "number of threads used to write tiles - only supported by " + Stream.of(TileArchiveConfig.Format.values())
          .filter(TileArchiveConfig.Format::supportsConcurrentWrites).map(TileArchiveConfig.Format::id).toList(),
        1))
      .logInterval(logging.getDuration("loginterval", "time between progress lines", "10s"))
      .minzoom(minzoom)
      .maxzoom(maxzoom)
      .maxzoomForRendering(renderMaxzoom)
      .force(output.getBoolean("force|-f", "overwrite the output and skip resource checks", false))
      .append(outputAdvanced.getBoolean(
        "append",
        "append to the output file - only supported by " + Stream.of(TileArchiveConfig.Format.values())
          .filter(TileArchiveConfig.Format::supportsAppend).map(TileArchiveConfig.Format::id).toList(),
        false))
      .compressTempStorage(processing.getBoolean(
        "compress_temp|gzip_temp",
        "compress temporary features (less disk, more CPU)",
        false))
      .mmapTempStorage(processingAdvanced.getBoolean("mmap_temp", "use memory-mapped IO for temp feature files", true))
      .sortMaxReaders(processingAdvanced.getInteger(
        "sort_max_readers",
        "maximum number of concurrent read threads to use when sorting chunks",
        parallelTempIO ? featureProcessThreads : 6))
      .sortMaxWriters(processingAdvanced.getInteger(
        "sort_max_writers",
        "maximum number of concurrent write threads to use when sorting chunks",
        parallelTempIO ? featureProcessThreads : 6))
      .nodeMapType(processing.getString(
        "nodemap_type",
        "node location index: " + Stream.of(LongLongMap.Type.values()).map(LongLongMap.Type::id)
          .collect(Collectors.joining(", ")) + "; array for the planet",
        LongLongMap.Type.SPARSE_ARRAY.id()))
      .nodeMapStorage(processing.getString(
        "nodemap_storage",
        "node location index storage: " + Stream.of(Storage.values()).map(Storage::id)
          .collect(Collectors.joining(", ")),
        defaultTempStorage))
      .nodeMapMadvise(processingAdvanced.getBoolean(
        "nodemap_madvise",
        "use linux madvise(random) for node locations",
        defaultMadvise))
      .multipolygonGeometryStorage(processingAdvanced.getString(
        "multipolygon_geometry_storage",
        "storage for multipolygon geometries, one of " + Stream.of(Storage.values()).map(Storage::id).toList(),
        defaultTempStorage))
      .multipolygonGeometryMadvise(processingAdvanced.getBoolean(
        "multipolygon_geometry_madvise",
        "use linux madvise(random) for temporary multipolygon geometry storage",
        defaultMadvise))
      .httpUserAgent(downloads.getString(
        "http_user_agent",
        "user-Agent header to set when downloading files over HTTP",
        "Planetiler downloader (https://github.com/onthegomap/planetiler)"))
      .httpTimeout(downloads.getDuration("http_timeout", "timeout to use when downloading files over HTTP", "30s"))
      .httpRetries(downloads.getInteger("http_retries", "retries to use when downloading files over HTTP", 5))
      .httpRetryWait(downloads.getDuration("http_retry_wait", "how long to wait before retrying HTTP request", "5s"))
      .httpRetryMaxWait(downloads.getDuration(
        "http_retry_max_wait",
        "longest a server may make us wait (Retry-After) before retrying an HTTP request, longer waits get capped",
        "5m"))
      .downloadChunkSizeMB(
        downloads.getLong("download_chunk_size_mb", "largest HTTP request to make when downloading in megabytes", 100))
      .downloadThreads(downloads.getInteger(
        "download_threads",
        "number of parallel requests to make to each remote file (up to 4x as many when reading part of a file)",
        4))
      .downloadMaxBandwidth(Parse.bandwidth(downloads.getString(
        "download_max_bandwidth",
        "maximum bandwidth to consume when downloading files in units mb/s, mbps, kbps, etc.",
        "")))
      .downloadMaxRequests(downloads.getInteger(
        "download_max_requests",
        "maximum number of HTTP requests in flight at once across all downloads (at least download_threads)",
        64))
      .downloadFailIfChanged(downloads.getBoolean(
        "download_fail_if_changed",
        "fail when the ETag or Last-Modified header of a remote file changes mid-download",
        true))
      .minFeatureSizeAtMaxZoom(outputAdvanced.getDouble(
        "min_feature_size_at_max_zoom",
        "default value for the minimum size in tile pixels of features to emit at the maximum zoom level to allow for overzooming",
        256d / 4096))
      .minFeatureSizeBelowMaxZoom(outputAdvanced.getDouble(
        "min_feature_size",
        "default value for the minimum size in tile pixels of features to emit below the maximum zoom level",
        1))
      .simplifyToleranceAtMaxZoom(outputAdvanced.getDouble(
        "simplify_tolerance_at_max_zoom",
        "default value for the tile pixel tolerance to use when simplifying features at the maximum zoom level to allow for overzooming",
        256d / 4096))
      .simplifyToleranceBelowMaxZoom(outputAdvanced.getDouble(
        "simplify_tolerance",
        "default value for the tile pixel tolerance to use when simplifying features below the maximum zoom level",
        0.1d))
      .osmLazyReads(processingAdvanced.getBoolean(
        "osm_lazy_reads",
        "read OSM blocks from disk in worker threads",
        true))
      .skipFilledTiles(output.getBoolean(
        "skip_filled_tiles",
        "don't write tiles that only contain a polygon fill",
        false))
      .tileWarningSizeBytes((int) (debugging.getDouble(
        "tile_warning_size_mb",
        "maximum size in megabytes of a tile to emit a warning about",
        1d) * 1024 * 1024))
      .color(logging.getBooleanObject("color", "color the output (NO_COLOR turns it off)"))
      .keepUnzippedSources(arguments.getBoolean(
        ArgumentGroup.SOURCES,
        "keep_unzipped",
        "keep unzipped sources by default after reading",
        false))
      .tileCompression(TileCompression
        .fromId(output.getString(
          "tile_compression",
          "tile compression: " +
            TileCompression.availableValues().stream().map(TileCompression::id).collect(Collectors.joining(", ")),
          "gzip")))
      .tileFormat(TileFormat
        .fromId(output.getString(
          "tile_format",
          "tile format: " +
            TileFormat.availableValues().stream().map(TileFormat::id).collect(Collectors.joining(", ")),
          "mvt")))
      .excludeIds(output.getBoolean("exclude_ids", "leave feature ids out of the tiles", false))
      .mltFsst(outputAdvanced.getBoolean("mlt_fsst", "use FSST encoding when tile format=MLT", mltAdvanced))
      .mltFastPfor(outputAdvanced.getBoolean("mlt_fastpfor", "use FastPFOR encoding when tile format=MLT", mltAdvanced))
      .mltTessellatePolygons(outputAdvanced.getBoolean(
        "mlt_tessellate_polygons",
        "pre-triangulate polygons when tile format=MLT so that clients do not need to",
        false))
      .mltPolygonOutline(outputAdvanced.getBoolean(
        "mlt_polygon_outline",
        "store polygon outlines together with pre-triangulated polygons",
        false))
      .mltReorderFeature(outputAdvanced.getBoolean(
        "mlt_reorder_features",
        "allow re-ordering output features within each layer when tile format=MLT to reduce tile sizes",
        false))
      .mltSharedDictionaries(outputAdvanced.getBoolean(
        "mlt_shared_dict",
        "share dictionaries between string fields when tile format=MLT",
        false))
      .outputLayerStats(
        output.getBoolean("output_layerstats", "also write per-tile, per-layer sizes (.layerstats.tsv.gz)", false))
      .debugUrlPattern(logging.getString(
        "debug_url",
        "debug url to use for displaying tiles with {z} {lat} {lon} placeholders",
        "https://onthegomap.github.io/planetiler-demo/#{z}/{lat}/{lon}"))
      .tmpDir(tmpDir)
      .tileWeights(outputAdvanced.file(
        "tile_weights",
        "tsv.gz file with columns z,x,y,loads to generate weighted average tile size stat",
        tmpDir.resolveSibling("tile_weights.tsv.gz")))
      .maxPointBuffer(outputAdvanced.getDouble(
        "max_point_buffer",
        "additional global limit for the max tile pixels to include points outside tile bounds of all layers. Set to a lower value to reduce tile size for " +
          "clients that handle label collisions across tiles (most web and native clients). NOTE: Do not reduce if you need to support " +
          "raster tile rendering",
        Double.POSITIVE_INFINITY))
      .logJtsExceptions(
        debugging.getBoolean("log_jts_exceptions", "emit verbose details to debug JTS geometry errors", false))
      .featureSourceIdMultiplier(outputAdvanced.getInteger(
        "feature_source_id_multiplier",
        "set vector tile feature IDs to (featureId * thisValue) + sourceId " +
          "where sourceId is 1 for OSM nodes, 2 for ways, 3 for relations, and 0 for other sources. Set to false to disable.",
        10))
      .extraNameTags(extraNameTags)
      .reuseFeatureDb(debugging.getBoolean(
        "reuse_featuredb",
        "reuse existing feature DB on disk, skipping source reading stages (for iterating on post-processing logic)",
        false))
      .parallelTempIO(parallelTempIO)
      .build();
  }

  public double minFeatureSize(int zoom) {
    return zoom >= maxzoomForRendering ? minFeatureSizeAtMaxZoom : minFeatureSizeBelowMaxZoom;
  }

  public double tolerance(int zoom) {
    return zoom >= maxzoomForRendering ? simplifyToleranceAtMaxZoom : simplifyToleranceBelowMaxZoom;
  }

  public Duration maxHttpRequestDuration() {
    // a server may ask to wait up to httpRetryMaxWait before each retry
    Duration retryWait = httpRetryWait.compareTo(httpRetryMaxWait) > 0 ? httpRetryWait : httpRetryMaxWait;
    return httpTimeout.multipliedBy(httpRetries + 1L).plus(retryWait.multipliedBy(httpRetries));
  }
}
