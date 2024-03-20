package com.onthegomap.planetiler.copy;

import com.onthegomap.planetiler.archive.TileArchiveConfig;
import com.onthegomap.planetiler.archive.TileArchiveMetadata;
import com.onthegomap.planetiler.archive.TileArchiveMetadataDeSer;
import com.onthegomap.planetiler.archive.TileCompression;
import com.onthegomap.planetiler.config.Arguments;
import com.onthegomap.planetiler.config.CommonConfigs;
import com.onthegomap.planetiler.geo.TileOrder;
import com.onthegomap.planetiler.stats.ProcessInfo;
import com.onthegomap.planetiler.stats.Stats;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.time.Duration;
import org.locationtech.jts.geom.Envelope;

record TileCopyConfig(
  TileArchiveConfig inArchive,
  TileArchiveConfig outArchive,
  Arguments inArguments,
  Arguments outArguments,
  TileCompression inCompression,
  TileCompression outCompression,
  int tileWriterThreads,
  int tileReaderThreads,
  int tileProcessorThreads,
  Duration logInterval,
  Stats stats,
  int queueSize,
  boolean append,
  boolean force,
  TileArchiveMetadata inMetadata,
  boolean skipEmpty,
  Envelope filterBounds,
  int filterMinzoom,
  int filterMaxzoom,
  boolean scanTilesInOrder,
  TileOrder outputTileOrder,
  int workQueueCapacity,
  int workQueueMaxBatch
) {

  TileCopyConfig {
    if (tileReaderThreads > 1 && !inArchive.format().supportsConcurrentReads()) {
      throw new IllegalArgumentException(inArchive.format().id() + " does not support concurrent reads");
    }
    if (tileWriterThreads > 1 && !outArchive.format().supportsConcurrentReads()) {
      throw new IllegalArgumentException(outArchive.format().id() + " does not support concurrent writes");
    }
    if (filterMinzoom > filterMaxzoom) {
      throw new IllegalArgumentException("require minzoom <= maxzoom");
    }
  }

  static TileCopyConfig fromArguments(Arguments baseArguments) {

    final Arguments inArguments = baseArguments.withPrefix("in");
    final Arguments outArguments = baseArguments.withPrefix("out");
    final Arguments baseOrOutArguments = outArguments.orElse(baseArguments);

    final Path inMetadataPath = inArguments.file("metadata", "path to metadata.json to use instead", null);
    final TileArchiveMetadata inMetadata;
    if (inMetadataPath != null) {
      try {
        inMetadata =
          TileArchiveMetadataDeSer.mbtilesMapper().readValue(inMetadataPath.toFile(), TileArchiveMetadata.class);
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    } else {
      inMetadata = null;
    }

    return new TileCopyConfig(
      TileArchiveConfig.from(baseArguments.getString("input", "input tile archive")),
      TileArchiveConfig.from(baseArguments.getString("output", "output tile archive")),
      inArguments,
      outArguments,
      getTileCompressionArg(inArguments, "the input tile compression"),
      getTileCompressionArg(outArguments, "the output tile compression"),
      CommonConfigs.tileWriterThreads(baseOrOutArguments),
      baseArguments.getInteger("tile_read_threads", "number of threads used to read tile data", 1),
      baseArguments.getInteger("tile_process_threads", "number of threads used to process tile data",
        Math.max(1, Runtime.getRuntime().availableProcessors() - 2)),
      CommonConfigs.logInterval(baseArguments),
      baseArguments.getStats(),
      Math.max(100, (int) (5_000d * ProcessInfo.getMaxMemoryBytes() / 100_000_000_000d)),
      CommonConfigs.appendToArchive(baseOrOutArguments),
      CommonConfigs.force(baseOrOutArguments),
      inMetadata,
      baseArguments.getBoolean("skip_empty", "skip empty (null/zero-bytes) tiles", true),
      baseArguments.bounds("filter_bounds", "the bounds to filter"),
      baseArguments.getInteger("filter_minzoom", "the min zoom", 0),
      baseArguments.getInteger("filter_maxzoom", "the max zoom", 14),
      baseArguments.getBoolean("scan_tiles_in_order", "output the tiles in the same order they are in the", true),
      baseArguments.getObject("output_tile_order", "the output tile order (if not scanned)", null,
        s -> s == null ? null : TileOrder.valueOf(s.toUpperCase())),
      100_000,
      100
    );
  }

  private static TileCompression getTileCompressionArg(Arguments args, String description) {
    return args.getObject("tile_compression", description, TileCompression.UNKNOWN,
      v -> TileCompression.findById(v).orElseThrow());
  }
}
