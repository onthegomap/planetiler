package com.onthegomap.flatmap.write;

import com.onthegomap.flatmap.CommonParams;
import com.onthegomap.flatmap.FileUtils;
import com.onthegomap.flatmap.Format;
import com.onthegomap.flatmap.LayerStats;
import com.onthegomap.flatmap.Profile;
import com.onthegomap.flatmap.VectorTileEncoder;
import com.onthegomap.flatmap.collections.FeatureGroup;
import com.onthegomap.flatmap.monitoring.Counter;
import com.onthegomap.flatmap.monitoring.ProgressLoggers;
import com.onthegomap.flatmap.monitoring.Stats;
import com.onthegomap.flatmap.worker.Topology;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.LongAccumulator;
import java.util.function.Consumer;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.util.zip.GZIPOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MbtilesWriter {

  private static final Logger LOGGER = LoggerFactory.getLogger(MbtilesWriter.class);

  private final Counter.Readable featuresProcessed;
  private final Counter memoizedTiles;
  private final Mbtiles db;
  private final CommonParams config;
  private final Profile profile;
  private final Stats stats;
  private final LayerStats layerStats;

  private final Counter.Readable[] tilesByZoom;
  private final Counter.Readable[] totalTileSizesByZoom;
  private final LongAccumulator[] maxTileSizesByZoom;
  private final FeatureGroup features;

  MbtilesWriter(FeatureGroup features, Mbtiles db, CommonParams config, Profile profile, Stats stats,
    LayerStats layerStats) {
    this.features = features;
    this.db = db;
    this.config = config;
    this.profile = profile;
    this.stats = stats;
    this.layerStats = layerStats;
    tilesByZoom = IntStream.rangeClosed(0, config.maxzoom()).mapToObj(i -> Counter.newSingleThreadCounter())
      .toArray(Counter.Readable[]::new);
    totalTileSizesByZoom = IntStream.rangeClosed(0, config.maxzoom()).mapToObj(i -> Counter.newMultiThreadCounter())
      .toArray(Counter.Readable[]::new);
    maxTileSizesByZoom = IntStream.rangeClosed(0, config.maxzoom()).mapToObj(i -> new LongAccumulator(Long::max, 0))
      .toArray(LongAccumulator[]::new);
    memoizedTiles = stats.longCounter("mbtiles_memoized_tiles");
    featuresProcessed = stats.longCounter("mbtiles_features_processed");
    Map<String, Counter.Readable> countsByZoom = new LinkedHashMap<>();
    for (int zoom = config.minzoom(); zoom <= config.maxzoom(); zoom++) {
      countsByZoom.put(Integer.toString(zoom), tilesByZoom[zoom]);
    }
    stats.counter("mbtiles_tiles_written", "zoom", () -> countsByZoom);
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
    var timer = stats.startTimer("mbtiles");
    MbtilesWriter writer = new MbtilesWriter(features, output, config, profile, stats,
      features.layerStats());

    var topology = Topology.start("mbtiles", stats)
      .fromGenerator("reader", writer::readFeatures, 1)
      .addBuffer("reader_queue", 50_000, 1_000)
      .addWorker("encoder", config.threads(), writer::tileEncoder)
      .addBuffer("writer_queue", 50_000, 1_000)
      .sinkTo("writer", 1, writer::tileWriter);

    var loggers = new ProgressLoggers("mbtiles")
      .addRatePercentCounter("features", features.numFeatures(), writer.featuresProcessed)
      .addRateCounter("tiles", writer::tilesEmitted)
      .addFileSize(fileSize)
      .add(" features ").addFileSize(features::getStorageSize)
      .addProcessStats()
      .addTopologyStats(topology);

    topology.awaitAndLog(loggers, config.logInterval());
    timer.stop();
  }

  void readFeatures(Consumer<FeatureGroup.TileFeatures> next) {
    int currentZoom = Integer.MIN_VALUE;
    for (var feature : features) {
      int z = feature.coord().z();
      if (z > currentZoom) {
        LOGGER.info("[mbtiles] Starting z" + z);
        currentZoom = z;
      }
      next.accept(feature);
    }
  }

  void tileEncoder(Supplier<FeatureGroup.TileFeatures> prev, Consumer<Mbtiles.TileEntry> next) throws IOException {
    FeatureGroup.TileFeatures tileFeatures, last = null;
    byte[] lastBytes = null, lastEncoded = null;
    while ((tileFeatures = prev.get()) != null) {
      featuresProcessed.incBy(tileFeatures.getNumFeatures());
      byte[] bytes, encoded;
      if (tileFeatures.hasSameContents(last)) {
        bytes = lastBytes;
        encoded = lastEncoded;
        memoizedTiles.inc();
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
      int zoom = tileFeatures.coord().z();
      int encodedLength = encoded.length;
      totalTileSizesByZoom[zoom].incBy(encodedLength);
      maxTileSizesByZoom[zoom].accumulate(encodedLength);
      next.accept(new Mbtiles.TileEntry(tileFeatures.coord(), bytes));
    }
  }

  private void tileWriter(Supplier<Mbtiles.TileEntry> tiles) {
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
      .setType(profile.isOverlay() ? "overlay" : "baselayer")
      .setBoundsAndCenter(config.latLonBounds())
      .setMinzoom(config.minzoom())
      .setMaxzoom(config.maxzoom())
      .setJson(layerStats.getTileStats());

    try (var batchedWriter = db.newBatchedTileWriter()) {
      Mbtiles.TileEntry tile;
      while ((tile = tiles.get()) != null) {
        int z = tile.tile().z();
        batchedWriter.write(tile.tile(), tile.bytes());
        stats.wroteTile(z, tile.bytes().length);
        tilesByZoom[z].inc();
      }
    }

    if (config.deferIndexCreation()) {
      db.addIndex();
    }
    if (config.optimizeDb()) {
      db.vacuumAnalyze();
    }

    printTileStats();
  }

  private void printTileStats() {
    LOGGER.debug("Tile stats:");
    long sumSize = 0;
    long sumCount = 0;
    long maxMax = 0;
    for (int z = config.minzoom(); z <= config.maxzoom(); z++) {
      long totalCount = tilesByZoom[z].get();
      long totalSize = totalTileSizesByZoom[z].get();
      sumSize += totalSize;
      sumCount += totalCount;
      long maxSize = maxTileSizesByZoom[z].get();
      LOGGER.debug("z" + z +
        " avg:" + Format.formatStorage(totalSize / Math.max(totalCount, 1), false) +
        " max:" + Format.formatStorage(maxSize, false));
    }
    LOGGER.debug("all" +
      " avg:" + Format.formatStorage(sumSize / Math.max(sumCount, 1), false) +
      " max:" + Format.formatStorage(maxMax, false));
    LOGGER.debug(" # features: " + Format.formatInteger(featuresProcessed.get()));
    LOGGER.debug("    # tiles: " + Format.formatInteger(this.tilesEmitted()));
  }

  private long tilesEmitted() {
    return Stream.of(tilesByZoom).mapToLong(Counter.Readable::get).sum();
  }

  private static byte[] gzipCompress(byte[] uncompressedData) throws IOException {
    var bos = new ByteArrayOutputStream(uncompressedData.length);
    try (var gzipOS = new GZIPOutputStream(bos)) {
      gzipOS.write(uncompressedData);
    }
    return bos.toByteArray();
  }

}
