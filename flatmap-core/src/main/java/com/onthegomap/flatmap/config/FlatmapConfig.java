package com.onthegomap.flatmap.config;

import java.time.Duration;

/**
 * Holder for common parameters used by many components in flatmap.
 */
public record FlatmapConfig(
  Arguments arguments,
  Bounds bounds,
  int threads,
  Duration logInterval,
  int minzoom,
  int maxzoom,
  boolean deferIndexCreation,
  boolean optimizeDb,
  boolean emitTilesInOrder,
  boolean forceOverwrite,
  boolean gzipTempStorage,
  String nodeMapType,
  String nodeMapStorage,
  String httpUserAgent,
  Duration httpTimeout,
  long downloadChunkSizeMB,
  int downloadThreads,
  double minFeatureSizeAtMaxZoom,
  double minFeatureSizeBelowMaxZoom,
  double simplifyToleranceAtMaxZoom,
  double simplifyToleranceBelowMaxZoom
) {

  public static final int MIN_MINZOOM = 0;
  public static final int MAX_MAXZOOM = 14;

  public FlatmapConfig {
    if (minzoom > maxzoom) {
      throw new IllegalArgumentException("Minzoom cannot be greater than maxzoom");
    }
    if (minzoom < MIN_MINZOOM) {
      throw new IllegalArgumentException("Minzoom must be >= " + MIN_MINZOOM + ", was " + minzoom);
    }
    if (maxzoom > MAX_MAXZOOM) {
      throw new IllegalArgumentException("Max zoom must be <= " + MAX_MAXZOOM + ", was " + maxzoom);
    }
  }

  public static FlatmapConfig defaults() {
    return from(Arguments.of());
  }

  public static FlatmapConfig from(Arguments arguments) {
    return new FlatmapConfig(
      arguments,
      new Bounds(arguments.bounds("bounds", "bounds")),
      arguments.threads(),
      arguments.getDuration("loginterval", "time between logs", "10s"),
      arguments.getInteger("minzoom", "minimum zoom level", MIN_MINZOOM),
      arguments.getInteger("maxzoom", "maximum zoom level (limit 14)", MAX_MAXZOOM),
      arguments.getBoolean("defer_mbtiles_index_creation", "skip adding index to mbtiles file", false),
      arguments.getBoolean("optimize_db", "optimize mbtiles after writing", false),
      arguments.getBoolean("emit_tiles_in_order", "emit tiles in index order", true),
      arguments.getBoolean("force", "force overwriting output file", false),
      arguments.getBoolean("gzip_temp", "gzip temporary feature storage (uses more CPU, but less disk space)", false),
      arguments
        .getString("nodemap_type", "type of node location map: noop, sortedtable, or sparsearray", "sortedtable"),
      arguments.getString("nodemap_storage", "storage for location map: mmap or ram", "mmap"),
      arguments.getString("http_user_agent", "User-Agent header to set when downloading files over HTTP",
        "Flatmap downloader (https://github.com/onthegomap/flatmap)"),
      arguments.getDuration("http_timeout", "Timeout to use when downloading files over HTTP", "30s"),
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
        0.1d)
    );
  }

  public double minFeatureSize(int zoom) {
    return zoom >= maxzoom ? minFeatureSizeAtMaxZoom : minFeatureSizeBelowMaxZoom;
  }

  public double tolerance(int zoom) {
    return zoom >= maxzoom ? simplifyToleranceAtMaxZoom : simplifyToleranceBelowMaxZoom;
  }
}
