package com.onthegomap.planetiler.config;

import com.onthegomap.planetiler.collection.LongLongMap;
import com.onthegomap.planetiler.collection.Storage;
import java.time.Duration;
import java.util.stream.Stream;

/**
 * Holder for common parameters used by many components in planetiler.
 */
public record PlanetilerConfig(
  Arguments arguments,
  Bounds bounds,
  int threads,
  Duration logInterval,
  int minzoom,
  int maxzoom,
  boolean deferIndexCreation,
  boolean optimizeDb,
  boolean emitTilesInOrder,
  boolean force,
  boolean gzipTempStorage,
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
  long downloadChunkSizeMB,
  int downloadThreads,
  double minFeatureSizeAtMaxZoom,
  double minFeatureSizeBelowMaxZoom,
  double simplifyToleranceAtMaxZoom,
  double simplifyToleranceBelowMaxZoom,
  boolean osmLazyReads
) {

  public static final int MIN_MINZOOM = 0;
  public static final int MAX_MAXZOOM = 14;

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
    return new PlanetilerConfig(
      arguments,
      new Bounds(arguments.bounds("bounds", "bounds")),
      arguments.threads(),
      arguments.getDuration("loginterval", "time between logs", "10s"),
      arguments.getInteger("minzoom", "minimum zoom level", MIN_MINZOOM),
      arguments.getInteger("maxzoom", "maximum zoom level (limit 14)", MAX_MAXZOOM),
      arguments.getBoolean("defer_mbtiles_index_creation", "skip adding index to mbtiles file", false),
      arguments.getBoolean("optimize_db", "optimize mbtiles after writing", false),
      arguments.getBoolean("emit_tiles_in_order", "emit tiles in index order", true),
      arguments.getBoolean("force", "overwriting output file and ignore disk/RAM warnings", false),
      arguments.getBoolean("gzip_temp", "gzip temporary feature storage (uses more CPU, but less disk space)", false),
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
        "storage for multipolygon geometries, one of " + Stream.of(Storage.values()).map(
          Storage::id).toList(),
        defaultTempStorage),
      arguments.getBoolean("multipolygon_geometry_madvise",
        "use linux madvise(random) for temporary multipolygon geometry storage",
        false),
      arguments.getString("http_user_agent", "User-Agent header to set when downloading files over HTTP",
        "Planetiler downloader (https://github.com/onthegomap/planetiler)"),
      arguments.getDuration("http_timeout", "Timeout to use when downloading files over HTTP", "30s"),
      arguments.getInteger("http_retries", "Retries to use when downloading files over HTTP", 1),
      arguments.getLong("download_chunk_size_mb", "Size of file chunks to download in parallel in megabytes", 100),
      arguments.getInteger("download_threads", "Number of parallel threads to use when downloading each file", 1),
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
        false)
    );
  }

  public double minFeatureSize(int zoom) {
    return zoom >= maxzoom ? minFeatureSizeAtMaxZoom : minFeatureSizeBelowMaxZoom;
  }

  public double tolerance(int zoom) {
    return zoom >= maxzoom ? simplifyToleranceAtMaxZoom : simplifyToleranceBelowMaxZoom;
  }
}
