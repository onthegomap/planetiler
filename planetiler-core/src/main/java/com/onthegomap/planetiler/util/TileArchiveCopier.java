package com.onthegomap.planetiler.util;

import com.onthegomap.planetiler.archive.TileArchiveConfig;
import com.onthegomap.planetiler.archive.TileArchives;
import com.onthegomap.planetiler.archive.TileEncodingResult;
import com.onthegomap.planetiler.config.Arguments;
import com.onthegomap.planetiler.config.PlanetilerConfig;
import com.onthegomap.planetiler.geo.GeoUtils;
import com.onthegomap.planetiler.geo.TileCoord;
import com.onthegomap.planetiler.geo.TileExtents;
import com.onthegomap.planetiler.stats.ProgressLoggers;
import java.io.IOException;
import java.util.OptionalLong;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;
import org.locationtech.jts.geom.Envelope;
import org.roaringbitmap.RoaringBitmap;

public class TileArchiveCopier {

  public static void main(String[] args) throws IOException {
    var arguments = Arguments.fromEnvOrArgs(args);
    var config = PlanetilerConfig.from(arguments);
    var source = arguments.getString("source", "source tile archive");
    var dest = arguments.getString("dest", "destination tile archive");
    var deduplicate = arguments.getBoolean("deduplicate", "attempt to deduplicate repeated tiles", true);
    copy(source, dest, config, deduplicate);
  }

  public static void copy(
    String source,
    String dest,
    PlanetilerConfig config,
    boolean deduplicate
  ) throws IOException {
    copy(
      TileArchiveConfig.from(source),
      TileArchiveConfig.from(dest),
      config,
      deduplicate
    );
  }

  public static void copy(
    TileArchiveConfig source,
    TileArchiveConfig dest,
    PlanetilerConfig config,
    boolean deduplicate
  ) throws IOException {
    try (
      var reader = TileArchives.newReader(source, config);
      var writer = TileArchives.newWriter(dest, config);
    ) {
      var tileOrder = writer.tileOrder();
      var metadata = reader.metadata();
      int minzoom = metadata.minzoom() != null ? Math.max(metadata.minzoom(), config.minzoom()) : config.minzoom();
      int maxzoom = metadata.maxzoom() != null ? Math.min(metadata.maxzoom(), config.maxzoom()) : config.maxzoom();
      Envelope envelope = metadata.bounds();
      TileExtents boundsFromConfig = config.bounds().tileExtents();
      TileExtents boundsFromReader =
        envelope != null ? TileExtents.computeFromWorldBounds(maxzoom, GeoUtils.toWorldBounds(envelope)) : null;
      Predicate<TileCoord> tileTest = boundsFromReader == null ?
        boundsFromConfig :
        tile -> boundsFromReader.test(tile) && boundsFromConfig.test(tile);

      writer.initialize(metadata);

      RoaringBitmap bitmap = new RoaringBitmap();
      try (var iter = reader.getAllTileCoords()) {
        while (iter.hasNext()) {
          var coord = iter.next();
          if (tileTest.test(coord)) {
            bitmap.add(tileOrder.encode(coord));
          }
        }
      }
      bitmap.runOptimize();

      try (var tileWriter = writer.newTileWriter()) {
        AtomicLong num = new AtomicLong();
        var logger = ProgressLoggers.create()
          .addRatePercentCounter("tiles", bitmap.getCardinality(), num::get, true);
        for (int index : bitmap) {
          num.incrementAndGet();
          if (index % 10_000_000 == 0) {
            logger.log();
          }
          var coord = tileOrder.decode(index);
          var tileData = reader.getTile(coord);
          if (tileData != null) {
            var result = new TileEncodingResult(
              coord,
              tileData,
              deduplicate && writer.deduplicates() ? OptionalLong.of(Hashing.fnv1a64(tileData)) : OptionalLong.empty()
            );
            tileWriter.write(result);
          }
        }
        tileWriter.printStats();
      }

      writer.finish(metadata);
    }
  }
}
