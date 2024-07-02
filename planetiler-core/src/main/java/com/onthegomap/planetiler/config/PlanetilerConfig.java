package com.onthegomap.planetiler.config;

import com.onthegomap.planetiler.archive.TileArchiveConfig;
import com.onthegomap.planetiler.archive.TileCompression;
import com.onthegomap.planetiler.collection.LongLongMap;
import com.onthegomap.planetiler.collection.Storage;
import com.onthegomap.planetiler.reader.osm.PolyFileReader;
import com.onthegomap.planetiler.util.MinioUtils;
import com.onthegomap.planetiler.util.Parse;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import org.apache.commons.lang3.StringUtils;

/**
 * Holder for common parameters used by many components in planetiler.
 */
public record PlanetilerConfig(
  // todo linespace
  ThreadPoolExecutor oosThreadPoolExecutor,
  MinioUtils minioUtils,
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
  long downloadChunkSizeMB,
  int downloadThreads,
  double downloadMaxBandwidth,
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
  boolean outputLayerStats,
  String debugUrlPattern,
  Path tmpDir,
  Path tileWeights,
  double maxPointBuffer,
  boolean logJtsExceptions,
  String oosSavePath,
  String outputType,
  String martinUrl,
  int featureSourceIdMultiplier
) {

  public static final int MIN_MINZOOM = 0;
  public static final int MAX_MAXZOOM = 15;
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
  }

  public static PlanetilerConfig defaults() {
    return from(Arguments.of());
  }

  public static PlanetilerConfig from(Arguments arguments) {
    // use --madvise and --storage options as default for temp storage, but allow users to override them explicitly for
    // multipolygon geometries or node locations
    boolean defaultMadvise =
      arguments.getBoolean("madvise",
        "default value for whether to use linux madvise(random) to improve memory-mapped read performance for temporary storage",
        true);
    // nodemap_storage was previously the only option, so if that's set make it the default
    String fallbackTempStorage = arguments.getArg("nodemap_storage", Storage.MMAP.id());
    String defaultTempStorage = arguments.getString("storage",
      "default storage type for temporary data, one of " + Stream.of(Storage.values()).map(
        Storage::id).toList(),
      fallbackTempStorage);
    int threads = arguments.threads();
    int featureWriteThreads =
      arguments.getInteger("write_threads", "number of threads to use when writing temp features",
        // defaults: <48 cpus=1 writer, 48-80=2 writers, 80-112=3 writers, 112-144=4 writers, ...
        Math.max(1, (threads - 16) / 32 + 1));
    int featureProcessThreads =
      arguments.getInteger("process_threads", "number of threads to use when processing input features",
        Math.max(threads < 8 ? threads : (threads - featureWriteThreads), 1));
    Bounds bounds = new Bounds(arguments.bounds("bounds", "bounds"));
    Path polygonFile =
      arguments.file("polygon", "a .poly file that limits output to tiles intersecting the shape", null);
    if (polygonFile != null) {
      try {
        bounds.setShape(PolyFileReader.parsePolyFile(polygonFile));
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }

    // todo linespace 创建文件上传线程池
    int processorsThreads = Runtime.getRuntime().availableProcessors() * 2 + 1;
    int oosCorePoolSize = arguments.getInteger("oosCorePoolSize", "文件上传核心线程数", 0);
    int oosMaxPoolSize = arguments.getInteger("oosMaxPoolSize", "文件上传最大线程数", 0);
    oosCorePoolSize = oosCorePoolSize == 0 ? processorsThreads : oosCorePoolSize;
    oosMaxPoolSize = oosMaxPoolSize == 0 ? processorsThreads : oosMaxPoolSize;
    if (oosMaxPoolSize < oosCorePoolSize) {
      oosMaxPoolSize = oosCorePoolSize;
    }

    ThreadPoolExecutor oosThreadPoolExecutor = new ThreadPoolExecutor(oosCorePoolSize, oosMaxPoolSize, 0L,
      TimeUnit.MINUTES,
      new LinkedBlockingQueue<>(),
      Executors.defaultThreadFactory(), new ThreadPoolExecutor.CallerRunsPolicy());

    // todo linespace 设置minio配置信息
    String accessKey = arguments.getString("accessKey", "accessKey", "");
    String secretKey = arguments.getString("secretKey", "secretKey", "");
    String endpoint = arguments.getString("endpoint", "endpoint", "");
    String bucketName = arguments.getString("bucketName", "bucketName", "");
    MinioUtils minioUtils;
    if (StringUtils.isNotBlank(accessKey) && StringUtils.isNotBlank(secretKey)
      && StringUtils.isNotBlank(endpoint) && StringUtils.isNotBlank(bucketName)) {
      minioUtils = new MinioUtils(endpoint, accessKey, secretKey, bucketName);
    } else {
      minioUtils = new MinioUtils();
    }

    int minzoom = arguments.getInteger("minzoom", "minimum zoom level", MIN_MINZOOM);
    int maxzoom = arguments.getInteger("maxzoom", "maximum zoom level up to " + MAX_MAXZOOM, DEFAULT_MAXZOOM);
    int renderMaxzoom =
      arguments.getInteger("render_maxzoom", "maximum rendering zoom level up to " + MAX_MAXZOOM,
        Math.max(maxzoom, DEFAULT_MAXZOOM));
    Path tmpDir = arguments.file("tmpdir", "temp directory", Path.of("data", "tmp"));

    return new PlanetilerConfig(
      // todo linespce
      oosThreadPoolExecutor,
      minioUtils,
      arguments,
      bounds,
      threads,
      featureWriteThreads,
      featureProcessThreads,
      arguments.getInteger("feature_read_threads", "number of threads to use when reading features at tile write time",
        threads < 32 ? 1 : 2),
      arguments.getInteger("tile_write_threads",
        "number of threads used to write tiles - only supported by " + Stream.of(TileArchiveConfig.Format.values())
          .filter(TileArchiveConfig.Format::supportsConcurrentWrites).map(TileArchiveConfig.Format::id).toList(),
        1),
      arguments.getDuration("loginterval", "time between logs", "10s"),
      minzoom,
      maxzoom,
      renderMaxzoom,
      arguments.getBoolean("force", "overwriting output file and ignore disk/RAM warnings", false),
      arguments.getBoolean("append",
        "append to the output file - only supported by " + Stream.of(TileArchiveConfig.Format.values())
          .filter(TileArchiveConfig.Format::supportsAppend).map(TileArchiveConfig.Format::id).toList(),
        false),
      arguments.getBoolean("compress_temp|gzip_temp",
        "compress temporary feature storage (uses more CPU, but less disk space)", false),
      arguments.getBoolean("mmap_temp", "use memory-mapped IO for temp feature files", true),
      arguments.getInteger("sort_max_readers", "maximum number of concurrent read threads to use when sorting chunks",
        6),
      arguments.getInteger("sort_max_writers", "maximum number of concurrent write threads to use when sorting chunks",
        6),
      arguments
        .getString("nodemap_type", "type of node location map, one of " + Stream.of(LongLongMap.Type.values()).map(
          t -> t.id()).toList(), LongLongMap.Type.SPARSE_ARRAY.id()),
      arguments.getString("nodemap_storage", "storage for node location map, one of " + Stream.of(Storage.values()).map(
        Storage::id).toList(), defaultTempStorage),
      arguments.getBoolean("nodemap_madvise", "use linux madvise(random) for node locations", defaultMadvise),
      arguments.getString("multipolygon_geometry_storage",
        "storage for multipolygon geometries, one of " + Stream.of(Storage.values()).map(Storage::id).toList(),
        defaultTempStorage),
      arguments.getBoolean("multipolygon_geometry_madvise",
        "use linux madvise(random) for temporary multipolygon geometry storage", defaultMadvise),
      arguments.getString("http_user_agent", "User-Agent header to set when downloading files over HTTP",
        "Planetiler downloader (https://github.com/onthegomap/planetiler)"),
      arguments.getDuration("http_timeout", "Timeout to use when downloading files over HTTP", "30s"),
      arguments.getInteger("http_retries", "Retries to use when downloading files over HTTP", 1),
      arguments.getDuration("http_retry_wait", "How long to wait before retrying HTTP request", "5s"),
      arguments.getLong("download_chunk_size_mb", "Size of file chunks to download in parallel in megabytes", 100),
      arguments.getInteger("download_threads", "Number of parallel threads to use when downloading each file", 1),
      Parse.bandwidth(arguments.getString("download_max_bandwidth",
        "Maximum bandwidth to consume when downloading files in units mb/s, mbps, kbps, etc.", "")),
      arguments.getDouble("min_feature_size_at_max_zoom",
        "Default value for the minimum size in tile pixels of features to emit at the maximum zoom level to allow for overzooming",
        256d / 4096),
      arguments.getDouble("min_feature_size",
        "Default value for the minimum size in tile pixels of features to emit below the maximum zoom level",
        1),
      arguments.getDouble("simplify_tolerance_at_max_zoom",
        "Default value for the tile pixel tolerance to use when simplifying features at the maximum zoom level to allow for overzooming",
        256d / 4096),
      arguments.getDouble("simplify_tolerance",
        "Default value for the tile pixel tolerance to use when simplifying features below the maximum zoom level",
        0.1d),
      arguments.getBoolean("osm_lazy_reads",
        "Read OSM blocks from disk in worker threads",
        true),
      arguments.getBoolean("skip_filled_tiles",
        "Skip writing tiles containing only polygon fills to the output",
        false),
      (int) (arguments.getDouble("tile_warning_size_mb",
        "Maximum size in megabytes of a tile to emit a warning about",
        1d) * 1024 * 1024),
      arguments.getBooleanObject("color", "Color the terminal output"),
      arguments.getBoolean("keep_unzipped",
        "keep unzipped sources by default after reading", false),
      TileCompression
        .fromId(arguments.getString("tile_compression",
          "the tile compression, one of " +
            TileCompression.availableValues().stream().map(TileCompression::id).toList(),
          "gzip")),
      arguments.getBoolean("output_layerstats", "output a tsv.gz file for each tile/layer size", false),
      arguments.getString("debug_url", "debug url to use for displaying tiles with {z} {lat} {lon} placeholders",
        "https://onthegomap.github.io/planetiler-demo/#{z}/{lat}/{lon}"),
      tmpDir,
      arguments.file("tile_weights", "tsv.gz file with columns z,x,y,loads to generate weighted average tile size stat",
        tmpDir.resolveSibling("tile_weights.tsv.gz")),
      arguments.getDouble("max_point_buffer",
        "Max tile pixels to include points outside tile bounds. Set to a lower value to reduce tile size for " +
          "clients that handle label collisions across tiles (most web and native clients). NOTE: Do not reduce if you need to support " +
          "raster tile rendering",
        Double.POSITIVE_INFINITY),
      arguments.getBoolean("log_jts_exceptions", "Emit verbose details to debug JTS geometry errors", false),
      // todo linespce
      arguments.getString("oossavepath", "存储服务器文件存储路径", ""),
      arguments.getString("outputType", "输出数据类型（mbtiles,pbf）", "pbf"),
      arguments.getString("martinUrl", "martin地址", "http://localhost:9545/"),
      arguments.getInteger("feature_source_id_multiplier",
        "Set vector tile feature IDs to (featureId * thisValue) + sourceId " +
          "where sourceId is 1 for OSM nodes, 2 for ways, 3 for relations, and 0 for other sources. Set to false to disable.",
        10)
    );
  }

  public double minFeatureSize(int zoom) {
    return zoom >= maxzoomForRendering ? minFeatureSizeAtMaxZoom : minFeatureSizeBelowMaxZoom;
  }

  public double tolerance(int zoom) {
    return zoom >= maxzoomForRendering ? simplifyToleranceAtMaxZoom : simplifyToleranceBelowMaxZoom;
  }
}
