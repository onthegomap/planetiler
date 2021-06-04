package com.onthegomap.flatmap.write;

import com.onthegomap.flatmap.CommonParams;
import com.onthegomap.flatmap.FileUtils;
import com.onthegomap.flatmap.LayerStats;
import com.onthegomap.flatmap.Profile;
import com.onthegomap.flatmap.VectorTileEncoder;
import com.onthegomap.flatmap.collections.FeatureGroup;
import com.onthegomap.flatmap.monitoring.ProgressLoggers;
import com.onthegomap.flatmap.monitoring.Stats;
import com.onthegomap.flatmap.worker.Topology;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import java.util.zip.GZIPOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MbtilesWriter {

  private static final Logger LOGGER = LoggerFactory.getLogger(MbtilesWriter.class);

  private final AtomicLong featuresProcessed = new AtomicLong(0);
  private final AtomicLong memoizedTiles = new AtomicLong(0);
  private final AtomicLong tilesEmitted = new AtomicLong(0);
  private final Mbtiles db;
  private final CommonParams config;
  private final Profile profile;
  private final Stats stats;
  private final LayerStats layerStats;

  MbtilesWriter(Mbtiles db, CommonParams config, Profile profile, Stats stats, LayerStats layerStats) {
    this.db = db;
    this.config = config;
    this.profile = profile;
    this.stats = stats;
    this.layerStats = layerStats;
  }

  public static void writeOutput(FeatureGroup features, Path outputPath, Profile profile, CommonParams config,
    Stats stats) {
    try (Mbtiles output = Mbtiles.newWriteToFileDatabase(outputPath)) {
      writeOutput(features, output, () -> FileUtils.fileSize(outputPath), profile, config, stats);
    } catch (IOException e) {
      throw new IllegalStateException("Unable to write to " + outputPath, e);
    }
  }

  public static void writeOutput(FeatureGroup features, Mbtiles output, LongSupplier fileSize, Profile profile,
    CommonParams config, Stats stats) {
    MbtilesWriter writer = new MbtilesWriter(output, config, profile, stats, features.layerStats());

    var topology = Topology.start("mbtiles", stats)
      .readFrom("reader", features)
      .addBuffer("reader_queue", 50_000, 1_000)
      .addWorker("encoder", config.threads(), writer::tileEncoder)
      .addBuffer("writer_queue", 50_000, 1_000)
      .sinkTo("writer", 1, writer::tileWriter);

    var loggers = new ProgressLoggers("mbtiles")
      .addRatePercentCounter("features", features.numFeatures(), writer.featuresProcessed)
      .addRateCounter("tiles", writer.tilesEmitted)
      .addFileSize(fileSize)
      .add(" features ").addFileSize(features::getStorageSize)
      .addProcessStats()
      .addTopologyStats(topology);

    topology.awaitAndLog(loggers, config.logInterval());
  }

  void tileEncoder(Supplier<FeatureGroup.TileFeatures> prev, Consumer<Mbtiles.TileEntry> next) throws IOException {
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
      next.accept(new Mbtiles.TileEntry(tileFeatures.coord(), bytes));
    }
  }

  private void tileWriter(Supplier<Mbtiles.TileEntry> tiles) throws Exception {
    db.setupSchema();
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
      .setBoundsAndCenter(config.latLonBounds())
      .setMinzoom(config.minzoom())
      .setMaxzoom(config.maxzoom())
      .setJson(layerStats.getTileStats());

    try (var batchedWriter = db.newBatchedTileWriter()) {
      Mbtiles.TileEntry tile;
      while ((tile = tiles.get()) != null) {
        batchedWriter.write(tile.tile(), tile.bytes());
        tilesEmitted.incrementAndGet();
      }
    }

    if (config.deferIndexCreation()) {
      db.addIndex();
    }
    if (config.optimizeDb()) {
      db.vacuumAnalyze();
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
