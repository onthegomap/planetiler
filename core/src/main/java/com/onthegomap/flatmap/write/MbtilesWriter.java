package com.onthegomap.flatmap.write;

import static com.onthegomap.flatmap.monitoring.ProgressLoggers.string;

import com.onthegomap.flatmap.CommonParams;
import com.onthegomap.flatmap.FileUtils;
import com.onthegomap.flatmap.Format;
import com.onthegomap.flatmap.LayerStats;
import com.onthegomap.flatmap.Profile;
import com.onthegomap.flatmap.VectorTileEncoder;
import com.onthegomap.flatmap.collections.FeatureGroup;
import com.onthegomap.flatmap.geo.TileCoord;
import com.onthegomap.flatmap.monitoring.Counter;
import com.onthegomap.flatmap.monitoring.ProgressLoggers;
import com.onthegomap.flatmap.monitoring.Stats;
import com.onthegomap.flatmap.worker.Topology;
import com.onthegomap.flatmap.worker.WorkQueue;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
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
  private final AtomicReference<TileCoord> lastTileWritten = new AtomicReference<>();
  private final AtomicLong batchLengths = new AtomicLong(0);
  private final AtomicLong batches = new AtomicLong(0);
  private final AtomicLong maxInputFeaturesPerTile = new AtomicLong(0);

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

    var topology = Topology.start("mbtiles", stats);

    int queueSize = 1_000;

    Topology<TileBatch> encodeBranch, writeBranch = null;
    if (config.emitTilesInOrder()) {
      WorkQueue<TileBatch> writerQueue = new WorkQueue<>("mbtiles_writer_queue", queueSize, 1, stats);
      encodeBranch = topology
        .<TileBatch>fromGenerator("reader", next -> writer.readFeatures(batch -> {
          next.accept(batch);
          writerQueue.accept(batch); // also send immediately to writer
        }), 1)
        .addBuffer("reader_queue", queueSize)
        .sinkTo("encoder", config.threads(), writer::tileEncoderSink);

      // the tile writer will wait on the result of each batch to ensure tiles are written in order
      writeBranch = topology.readFromQueue(writerQueue)
        .sinkTo("writer", 1, writer::tileWriter);
    } else {
      encodeBranch = topology
        .fromGenerator("reader", writer::readFeatures, 1)
        .addBuffer("reader_queue", queueSize)
        .addWorker("encoder", config.threads(), writer::tileEncoder)
        .addBuffer("writer_queue", queueSize)
        .sinkTo("writer", 1, writer::tileWriter);
    }

    var loggers = new ProgressLoggers("mbtiles")
      .addRatePercentCounter("features", features.numFeatures(), writer.featuresProcessed)
      .addRateCounter("tiles", writer::tilesEmitted)
      .addFileSize(fileSize)
      .add(" features ").addFileSize(features::getStorageSize)
      .addProcessStats()
      .addTopologyStats(encodeBranch)
      .addTopologyStats(writeBranch)
      .newLine()
      .add(string(() -> {
        TileCoord lastTile = writer.lastTileWritten.get();
        String blurb;
        long batches = writer.batches.getAndSet(0);
        long batchLen = writer.batchLengths.getAndSet(0);
        String avgBatch = batches == 0 ? "-" : Long.toString(batchLen / batches);
        if (lastTile == null) {
          blurb = "n/a";
        } else {
          blurb =
            lastTile.z() + "/" + lastTile.x() + "/" + lastTile.y() + " bsize:" + avgBatch + " max: "
              + writer.maxInputFeaturesPerTile.getAndSet(0) + " " + lastTile.getDebugUrl();
        }
        return "last tile: " + blurb;
      }));

    encodeBranch.awaitAndLog(loggers, config.logInterval());
    if (writeBranch != null) {
      writeBranch.awaitAndLog(loggers, config.logInterval());
    }
    writer.printTileStats();
    timer.stop();
  }

  private static record TileBatch(
    List<FeatureGroup.TileFeatures> in,
    CompletableFuture<Queue<Mbtiles.TileEntry>> out
  ) {

    TileBatch() {
      this(new ArrayList<>(), new CompletableFuture<>());
    }

    public int size() {
      return in.size();
    }

    public boolean isEmpty() {
      return in.isEmpty();
    }
  }

  void readFeatures(Consumer<TileBatch> next) {
    int currentZoom = Integer.MIN_VALUE;
    TileBatch batch = new TileBatch();
    long featuresInThisBatch = 0;
    long tilesInThisBatch = 0;
    // 249 vs. 24,900
    long MAX_FEATURES_PER_BATCH = 10_000;
    long MIN_TILES_PER_BATCH = 1;
    long MAX_TILES_PER_BATCH = 1_000;
    LOGGER.info("MAX_TILES_PER_BATCH=" + MAX_TILES_PER_BATCH);
    LOGGER.info("MAX_FEATURES_PER_BATCH=" + MAX_FEATURES_PER_BATCH);
    LOGGER.info("MIN_TILES_PER_BATCH=" + MIN_TILES_PER_BATCH);
    for (var feature : features) {
      int z = feature.coord().z();
      if (z > currentZoom) {
        LOGGER.info("[mbtiles] Starting z" + z);
        currentZoom = z;
      }
      if (tilesInThisBatch > MAX_TILES_PER_BATCH ||
        (tilesInThisBatch >= MIN_TILES_PER_BATCH && featuresInThisBatch > MAX_FEATURES_PER_BATCH)) {
        next.accept(batch);
        batch = new TileBatch();
        featuresInThisBatch = 0;
        tilesInThisBatch = 0;
      }
      featuresInThisBatch += feature.getNumFeatures();
      maxInputFeaturesPerTile.accumulateAndGet(feature.getNumFeatures(), Long::max);
      tilesInThisBatch++;
      batch.in.add(feature);
    }
    if (!batch.in.isEmpty()) {
      next.accept(batch);
    }
  }

  void tileEncoderSink(Supplier<TileBatch> prev) throws IOException {
    tileEncoder(prev, batch -> {
    });
  }

  void tileEncoder(Supplier<TileBatch> prev, Consumer<TileBatch> next) throws IOException {
    TileBatch batch;
    byte[] lastBytes = null, lastEncoded = null;

    while ((batch = prev.get()) != null) {
      Queue<Mbtiles.TileEntry> result = new ArrayDeque<>(batch.size());
      FeatureGroup.TileFeatures last = null;
      for (int i = 0; i < batch.in.size(); i++) {
        FeatureGroup.TileFeatures tileFeatures = batch.in.get(i);
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
            LOGGER.warn(tileFeatures.coord() + " " + (encoded.length / 1024) + "kb uncompressed");
          }
        }
        int zoom = tileFeatures.coord().z();
        int encodedLength = encoded.length;
        totalTileSizesByZoom[zoom].incBy(encodedLength);
        maxTileSizesByZoom[zoom].accumulate(encodedLength);
        result.add(new Mbtiles.TileEntry(tileFeatures.coord(), bytes));
      }
      batch.out.complete(result);
      next.accept(batch);
    }
  }

  private void tileWriter(Supplier<TileBatch> tileBatches) throws ExecutionException, InterruptedException {
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

    TileCoord lastTile = null;
    try (var batchedWriter = db.newBatchedTileWriter()) {
      TileBatch batch;
      while ((batch = tileBatches.get()) != null) {
        Queue<Mbtiles.TileEntry> tiles = batch.out.get();
        Mbtiles.TileEntry tile;
        long batchSize = 0;
        while ((tile = tiles.poll()) != null) {
          TileCoord tileCoord = tile.tile();
          assert lastTile == null || lastTile.compareTo(tileCoord) < 0;
          lastTile = tile.tile();
          int z = tileCoord.z();
          batchedWriter.write(tile.tile(), tile.bytes());
          stats.wroteTile(z, tile.bytes().length);
          tilesByZoom[z].inc();
          batchSize++;
        }
        batches.incrementAndGet();
        batchLengths.addAndGet(batchSize);
        lastTileWritten.set(lastTile);
      }
    }

    if (config.deferIndexCreation()) {
//      db.addIndex();
    }
    if (config.optimizeDb()) {
      db.vacuumAnalyze();
    }
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
