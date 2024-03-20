package com.onthegomap.planetiler.config;

import com.onthegomap.planetiler.archive.TileArchiveConfig;
import java.time.Duration;
import java.util.stream.Stream;

public final class CommonConfigs {
  private CommonConfigs() {}

  public static boolean force(Arguments arguments) {
    return arguments.getBoolean("force", "overwriting output file and ignore disk/RAM warnings", false);
  }

  public static boolean appendToArchive(Arguments arguments) {
    return arguments.getBoolean("append",
      "append to the output file - only supported by " + Stream.of(TileArchiveConfig.Format.values())
        .filter(TileArchiveConfig.Format::supportsAppend).map(TileArchiveConfig.Format::id).toList(),
      false);
  }

  public static int tileWriterThreads(Arguments arguments) {
    return arguments.getInteger("tile_write_threads",
      "number of threads used to write tiles - only supported by " + Stream.of(TileArchiveConfig.Format.values())
        .filter(TileArchiveConfig.Format::supportsConcurrentWrites).map(TileArchiveConfig.Format::id).toList(),
      1);
  }

  public static Duration logInterval(Arguments arguments) {
    return arguments.getDuration("loginterval", "time between logs", "10s");
  }
}
