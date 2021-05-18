package com.onthegomap.flatmap;

import com.onthegomap.flatmap.geo.GeoUtils;
import java.time.Duration;
import org.locationtech.jts.geom.Envelope;

public record CommonParams(
  // provided
  Envelope latLonBounds,
  int threads,
  Duration logInterval,
  int minzoom,
  int maxzoom,
  boolean deferIndexCreation,
  boolean optimizeDb,
  boolean forceOverwrite,

  // computed
  Envelope worldBounds,
  TileExtents extents
) {

  public CommonParams(
    Envelope latLonBounds,
    int threads,
    Duration logInterval,
    int minzoom,
    int maxzoom,
    boolean deferIndexCreation,
    boolean optimizeDb,
    boolean forceOverwrite
  ) {
    this(
      latLonBounds,
      threads,
      logInterval,
      minzoom,
      maxzoom,
      deferIndexCreation,
      optimizeDb,
      forceOverwrite,

      // computed
      GeoUtils.toWorldBounds(latLonBounds),
      TileExtents.computeFromWorldBounds(maxzoom, GeoUtils.toWorldBounds(latLonBounds))
    );
  }

  public static final int MIN_MINZOOM = 0;
  public static final int MAX_MAXZOOM = 14;

  public CommonParams {
    if (minzoom > maxzoom) {
      throw new IllegalArgumentException("Minzoom cannot be greater than maxzoom");
    }
    if (minzoom < 0) {
      throw new IllegalArgumentException("Minzoom must be >= 0, was " + minzoom);
    }
    if (maxzoom > 14) {
      throw new IllegalArgumentException("Max zoom must be <= 14, was " + maxzoom);
    }
  }

  public static CommonParams defaults() {
    return from(Arguments.empty());
  }

  public static CommonParams from(Arguments arguments) {
    return from(arguments, BoundsProvider.WORLD);
  }

  public static CommonParams from(Arguments arguments, BoundsProvider defaultBounds) {
    return new CommonParams(
      arguments.bounds("bounds", "bounds", defaultBounds),
      arguments.threads(),
      arguments.duration("loginterval", "time between logs", "10s"),
      arguments.integer("minzoom", "minimum zoom level", MIN_MINZOOM),
      arguments.integer("maxzoom", "maximum zoom level (limit 14)", MAX_MAXZOOM),
      arguments.get("defer_mbtiles_index_creation", "add index to mbtiles file after finished writing", false),
      arguments.get("optimize_db", "optimize mbtiles after writing", false),
      arguments.get("force", "force overwriting output file", false)
    );
  }
}
