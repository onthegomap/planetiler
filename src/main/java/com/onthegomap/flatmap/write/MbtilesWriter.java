package com.onthegomap.flatmap.write;

import com.onthegomap.flatmap.CommonParams;
import com.onthegomap.flatmap.Profile;
import com.onthegomap.flatmap.VectorTileEncoder;
import com.onthegomap.flatmap.collections.FeatureGroup;
import com.onthegomap.flatmap.geo.TileCoord;
import com.onthegomap.flatmap.monitoring.ProgressLoggers;
import com.onthegomap.flatmap.monitoring.Stats;
import com.onthegomap.flatmap.worker.Topology;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.zip.GZIPOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MbtilesWriter {

  private static final Logger LOGGER = LoggerFactory.getLogger(MbtilesWriter.class);

  private final AtomicLong featuresProcessed = new AtomicLong(0);
  private final AtomicLong memoizedTiles = new AtomicLong(0);
  private final AtomicLong tiles = new AtomicLong(0);
  private final Path path;
  private final CommonParams config;
  private final Profile profile;
  private final Stats stats;

  private MbtilesWriter(Path path, CommonParams config, Profile profile, Stats stats) {
    this.path = path;
    this.config = config;
    this.profile = profile;
    this.stats = stats;
  }

  private static record RenderedTile(TileCoord tile, byte[] contents) {

  }

  public static void writeOutput(long featureCount, FeatureGroup features, Path output, Profile profile,
    CommonParams config, Stats stats) {
    try {
      Files.deleteIfExists(output);
    } catch (IOException e) {
      throw new IllegalStateException("Unable to delete " + output);
    }
    MbtilesWriter writer = new MbtilesWriter(output, config, profile, stats);

    var topology = Topology.start("mbtiles", stats)
      .readFrom("reader", features)
      .addBuffer("reader_queue", 50_000, 1_000)
      .addWorker("encoder", config.threads(), writer::tileEncoder)
      .addBuffer("writer_queue", 50_000, 1_000)
      .sinkTo("writer", 1, writer::tileWriter);

    var loggers = new ProgressLoggers("mbtiles")
      .addRatePercentCounter("features", featureCount, writer.featuresProcessed)
      .addRateCounter("tiles", writer.tiles)
      .addFileSize(output)
      .add(" features ").addFileSize(features::getStorageSize)
      .addProcessStats()
      .addTopologyStats(topology);

    topology.awaitAndLog(loggers, config.logInterval());
  }

  public void tileEncoder(Supplier<FeatureGroup.TileFeatures> prev, Consumer<RenderedTile> next) throws Exception {
    FeatureGroup.TileFeatures tileFeatures, last = null;
    byte[] lastBytes = null, lastEncoded = null;
    while ((tileFeatures = prev.get()) != null) {
      featuresProcessed.addAndGet(tileFeatures.getNumFeatures());
      byte[] bytes, encoded;
      if (tileFeatures.hasSameContents(last)) {
        bytes = lastBytes;
        encoded = lastEncoded;
        memoizedTiles.incrementAndGet();
      } else {
        VectorTileEncoder en = tileFeatures.getTile();
        encoded = en.encode();
        bytes = gzipCompress(encoded);
        last = tileFeatures;
        lastEncoded = encoded;
        lastBytes = bytes;
        if (encoded.length > 1_000_000) {
          LOGGER.warn(tileFeatures.coord() + " " + encoded.length / 1024 + "kb uncompressed");
        }
      }
      stats.encodedTile(tileFeatures.coord().z(), encoded.length);
      next.accept(new RenderedTile(tileFeatures.coord(), bytes));
    }
  }

  private void tileWriter(Supplier<RenderedTile> tiles) throws Exception {
    try (Mbtiles db = Mbtiles.newFileDatabase(path)) {
      db.setupSchema();
      db.tuneForWrites();
      if (!config.deferIndexCreation()) {
        db.addIndex();
      } else {
        LOGGER.info("Deferring index creation until after tiles are written.");
      }

      db.metadata()
        .setName(profile.name())
        .setFormat("pbf")
        .setDescription(profile.description())
        .setAttribution(profile.attribution())
        .setVersion(profile.version())
        .setTypeIsBaselayer()
        .setBoundsAndCenter(config.bounds())
        .setMinzoom(config.minzoom())
        .setMaxzoom(config.maxzoom())
        .setJson(stats.getTileStats());

      try (var batchedWriter = db.newBatchedTileWriter()) {
        RenderedTile tile;
        while ((tile = tiles.get()) != null) {
          batchedWriter.write(tile.tile(), tile.contents);
        }
      }

      if (config.deferIndexCreation()) {
        db.addIndex();
      }
      if (config.optimizeDb()) {
        db.vacuumAnalyze();
      }
    }
  }

  private static byte[] gzipCompress(byte[] uncompressedData) throws IOException {
    var bos = new ByteArrayOutputStream(uncompressedData.length);
    try (var gzipOS = new GZIPOutputStream(bos)) {
      gzipOS.write(uncompressedData);
    }
    return bos.toByteArray();
  }

}
