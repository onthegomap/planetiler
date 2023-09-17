package com.onthegomap.planetiler.archive;

import static com.onthegomap.planetiler.util.Gzip.gzip;
import static com.onthegomap.planetiler.worker.Worker.joinFutures;

import com.onthegomap.planetiler.VectorTile;
import com.onthegomap.planetiler.collection.FeatureGroup;
import com.onthegomap.planetiler.config.PlanetilerConfig;
import com.onthegomap.planetiler.geo.TileCoord;
import com.onthegomap.planetiler.stats.Counter;
import com.onthegomap.planetiler.stats.ProcessInfo;
import com.onthegomap.planetiler.stats.ProgressLoggers;
import com.onthegomap.planetiler.stats.Stats;
import com.onthegomap.planetiler.stats.Timer;
import com.onthegomap.planetiler.util.DiskBacked;
import com.onthegomap.planetiler.util.Format;
import com.onthegomap.planetiler.util.Hashing;
import com.onthegomap.planetiler.util.TileSizeStats;
import com.onthegomap.planetiler.util.TilesetSummaryStatistics;
import com.onthegomap.planetiler.worker.WorkQueue;
import com.onthegomap.planetiler.worker.Worker;
import com.onthegomap.planetiler.worker.WorkerPipeline;
import java.io.IOException;
import java.nio.file.Path;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.LongSupplier;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Final stage of the map generation process that encodes vector tiles using {@link VectorTile} and writes them to a
 * {@link WriteableTileArchive}.
 */
public class TileArchiveWriter {

  private static final Logger LOGGER = LoggerFactory.getLogger(TileArchiveWriter.class);
  private static final long MAX_FEATURES_PER_BATCH = 10_000;
  private static final long MAX_TILES_PER_BATCH = 1_000;
  private final Counter.Readable featuresProcessed;
  private final Counter memoizedTiles;
  private final WriteableTileArchive archive;
  private final PlanetilerConfig config;
  private final Stats stats;
  private final Counter.Readable[] tilesByZoom;
  private final Iterable<FeatureGroup.TileFeatures> inputTiles;
  private final AtomicReference<TileCoord> lastTileWritten = new AtomicReference<>();
  private final TileArchiveMetadata tileArchiveMetadata;
  private final TilesetSummaryStatistics tileStats = new TilesetSummaryStatistics();

  private TileArchiveWriter(Iterable<FeatureGroup.TileFeatures> inputTiles, WriteableTileArchive archive,
    PlanetilerConfig config, TileArchiveMetadata tileArchiveMetadata, Stats stats) {
    this.inputTiles = inputTiles;
    this.archive = archive;
    this.config = config;
    this.tileArchiveMetadata = tileArchiveMetadata;
    this.stats = stats;
    tilesByZoom = IntStream.rangeClosed(0, config.maxzoom())
      .mapToObj(i -> Counter.newSingleThreadCounter())
      .toArray(Counter.Readable[]::new);
    memoizedTiles = stats.longCounter("archive_memoized_tiles");
    featuresProcessed = stats.longCounter("archive_features_processed");
    Map<String, LongSupplier> countsByZoom = new LinkedHashMap<>();
    for (int zoom = config.minzoom(); zoom <= config.maxzoom(); zoom++) {
      countsByZoom.put(Integer.toString(zoom), tilesByZoom[zoom]);
    }
    stats.counter("archive_tiles_written", "zoom", () -> countsByZoom);
  }

  /** Reads all {@code features}, encodes them in parallel, and writes to {@code output}. */
  public static void writeOutput(FeatureGroup features, WriteableTileArchive output, DiskBacked fileSize,
    TileArchiveMetadata tileArchiveMetadata, Path layerStatsPath, PlanetilerConfig config, Stats stats) {
    var timer = stats.startStage("archive");

    int readThreads = config.featureReadThreads();
    int threads = config.threads();
    int processThreads = threads < 10 ? threads : threads - readThreads;
    int tileWriteThreads = config.tileWriteThreads();

    // when using more than 1 read thread: (N read threads) -> (1 merge thread) -> ...
    // when using 1 read thread we just have: (1 read & merge thread) -> ...
    Worker readWorker = null;
    Iterable<FeatureGroup.TileFeatures> inputTiles;
    String secondStageName;
    if (readThreads == 1) {
      secondStageName = "read";
      inputTiles = features;
    } else {
      secondStageName = "merge";
      var reader = features.parallelIterator(readThreads);
      inputTiles = reader.result();
      readWorker = reader.readWorker();
    }

    TileArchiveWriter writer =
      new TileArchiveWriter(inputTiles, output, config, tileArchiveMetadata.withLayerStats(features.layerStats()
        .getTileStats()), stats);

    var pipeline = WorkerPipeline.start("archive", stats);

    // a larger tile queue size helps keep cores busy, but needs a lot of RAM
    // 5k works fine with 100GB of RAM, so adjust the queue size down from there
    // but no less than 100
    int queueSize = Math.max(
      100,
      (int) (5_000d * ProcessInfo.getMaxMemoryBytes() / 100_000_000_000d)
    );

    /*
     * To emit tiles in order, fork the input queue and send features to both the encoder and writer. The writer
     * waits on them to be encoded in the order they were received, and the encoder processes them in parallel.
     * One batch might take a long time to process, so make the queues very big to avoid idle encoding CPUs.
     *
     * Note:
     * In the future emitting tiles out order might be especially interesting when tileWriteThreads>1,
     * since when multiple threads/files are included there's no order that needs to be preserved.
     * So some of the restrictions could be lifted then.
     */
    WorkQueue<TileBatch> writerQueue = new WorkQueue<>("archive_writer_queue", queueSize, 1, stats);
    WorkQueue<TileBatch> layerStatsQueue = new WorkQueue<>("archive_layerstats_queue", queueSize, 1, stats);
    WorkerPipeline<TileBatch> encodeBranch = pipeline
      .<TileBatch>fromGenerator(secondStageName, next -> {
        try (writerQueue; layerStatsQueue) {
          var writerEnqueuer = writerQueue.threadLocalWriter();
          var statsEnqueuer = layerStatsQueue.threadLocalWriter();
          writer.readFeaturesAndBatch(batch -> {
            next.accept(batch);
            writerEnqueuer.accept(batch); // also send immediately to writer
            if (config.outputLayerStats()) {
              statsEnqueuer.accept(batch);
            }
          });
        }
        // use only 1 thread since readFeaturesAndBatch needs to be single-threaded
      }, 1)
      .addBuffer("reader_queue", queueSize)
      .sinkTo("encode", processThreads, writer::tileEncoderSink);

    // the tile writer will wait on the result of each batch to ensure tiles are written in order
    WorkerPipeline<TileBatch> writeBranch = pipeline.readFromQueue(writerQueue)
      .sinkTo("write", tileWriteThreads, writer::tileWriter);

    WorkerPipeline<TileBatch> layerStatsBranch = null;

    if (config.outputLayerStats()) {
      layerStatsBranch = pipeline.readFromQueue(layerStatsQueue)
        .sinkTo("stats", 1, tileStatsWriter(layerStatsPath));
    }

    var loggers = ProgressLoggers.create()
      .addRatePercentCounter("features", features.numFeaturesWritten(), writer.featuresProcessed, true)
      .addFileSize(features)
      .addRateCounter("tiles", writer::tilesEmitted)
      .addFileSize(fileSize)
      .newLine()
      .addProcessStats()
      .newLine();
    if (readWorker != null) {
      loggers.addThreadPoolStats("read", readWorker);
    }
    loggers.addPipelineStats(encodeBranch)
      .addPipelineStats(writeBranch);
    if (layerStatsBranch != null) {
      loggers.addPipelineStats(layerStatsBranch);
    }
    loggers.newLine()
      .newLine()
      .add(writer::getLastTileLogDetails);

    var doneFuture = joinFutures(
      writeBranch.done(),
      layerStatsBranch == null ? CompletableFuture.completedFuture(null) : layerStatsBranch.done(),
      encodeBranch.done());
    loggers.awaitAndLog(doneFuture, config.logInterval());
    writer.printTileStats();
    timer.stop();
  }

  private static WorkerPipeline.SinkStep<TileBatch> tileStatsWriter(Path layerStatsPath) {
    return prev -> {
      try (var statsWriter = TileSizeStats.newWriter(layerStatsPath)) {
        statsWriter.write(TileSizeStats.headerRow());
        for (var batch : prev) {
          for (var encodedTile : batch.out().get()) {
            for (var line : encodedTile.layerStats()) {
              statsWriter.write(line);
            }
          }
        }
      }
    };
  }

  private String getLastTileLogDetails() {
    TileCoord lastTile = lastTileWritten.get();
    String blurb;
    if (lastTile == null) {
      blurb = "n/a";
    } else {
      blurb = "%d/%d/%d (z%d %s) %s".formatted(
        lastTile.z(), lastTile.x(), lastTile.y(),
        lastTile.z(),
        Format.defaultInstance().percent(archive.tileOrder().progressOnLevel(lastTile, config.bounds().tileExtents())),
        lastTile.getDebugUrl(config.debugUrlPattern())
      );
    }
    return "last tile: " + blurb;
  }

  private void readFeaturesAndBatch(Consumer<TileBatch> next) {
    int currentZoom = Integer.MIN_VALUE;
    TileBatch batch = new TileBatch();
    long featuresInThisBatch = 0;
    long tilesInThisBatch = 0;
    for (var feature : inputTiles) {
      int z = feature.tileCoord().z();
      if (z != currentZoom) {
        LOGGER.trace("Starting z{}", z);
        currentZoom = z;
      }
      long thisTileFeatures = feature.getNumFeaturesToEmit();
      if (tilesInThisBatch > 0 &&
        (tilesInThisBatch >= MAX_TILES_PER_BATCH ||
          ((featuresInThisBatch + thisTileFeatures) > MAX_FEATURES_PER_BATCH))) {
        next.accept(batch);
        batch = new TileBatch();
        featuresInThisBatch = 0;
        tilesInThisBatch = 0;
      }
      featuresInThisBatch += thisTileFeatures;
      tilesInThisBatch++;
      batch.in.add(feature);
    }
    if (!batch.in.isEmpty()) {
      next.accept(batch);
    }
  }

  private void tileEncoderSink(Iterable<TileBatch> prev) throws IOException {
    /*
     * To optimize emitting many identical consecutive tiles (like large ocean areas), memoize output to avoid
     * recomputing if the input hasn't changed.
     */
    byte[] lastBytes = null, lastEncoded = null;
    Long lastTileDataHash = null;
    boolean lastIsFill = false;
    List<TileSizeStats.LayerStats> lastLayerStats = null;
    boolean skipFilled = config.skipFilledTiles();

    var tileStatsUpdater = tileStats.threadLocalUpdater();
    for (TileBatch batch : prev) {
      List<TileEncodingResult> result = new ArrayList<>(batch.size());
      FeatureGroup.TileFeatures last = null;
      // each batch contains tile ordered by tile-order ID ascending
      for (int i = 0; i < batch.in.size(); i++) {
        FeatureGroup.TileFeatures tileFeatures = batch.in.get(i);
        featuresProcessed.incBy(tileFeatures.getNumFeaturesProcessed());
        byte[] bytes, encoded;
        List<TileSizeStats.LayerStats> layerStats;
        Long tileDataHash;
        if (tileFeatures.hasSameContents(last)) {
          bytes = lastBytes;
          encoded = lastEncoded;
          tileDataHash = lastTileDataHash;
          layerStats = lastLayerStats;
          memoizedTiles.inc();
        } else {
          VectorTile en = tileFeatures.getVectorTileEncoder();
          if (skipFilled && (lastIsFill = en.containsOnlyFills())) {
            encoded = null;
            layerStats = null;
            bytes = null;
          } else {
            var proto = en.toProto();
            encoded = proto.toByteArray();
            bytes = switch (config.tileCompression()) {
              case GZIP -> gzip(encoded);
              case NONE -> encoded;
              case UNKNWON -> throw new IllegalArgumentException("cannot compress \"UNKNOWN\"");
            };
            layerStats = TileSizeStats.computeTileStats(proto);
            if (encoded.length > config.tileWarningSizeBytes()) {
              LOGGER.warn("{} {}kb uncompressed",
                tileFeatures.tileCoord(),
                encoded.length / 1024);
            }
          }
          lastLayerStats = layerStats;
          lastEncoded = encoded;
          lastBytes = bytes;
          last = tileFeatures;
          if (archive.deduplicates() && en.likelyToBeDuplicated() && bytes != null) {
            tileDataHash = generateContentHash(bytes);
          } else {
            tileDataHash = null;
          }
          lastTileDataHash = tileDataHash;
        }
        if ((skipFilled && lastIsFill) || bytes == null) {
          continue;
        }
        tileStatsUpdater.recordTile(tileFeatures.tileCoord(), bytes.length, layerStats);
        List<String> layerStatsRows = config.outputLayerStats() ?
          TileSizeStats.formatOutputRows(tileFeatures.tileCoord(), bytes.length, layerStats) :
          List.of();
        result.add(
          new TileEncodingResult(
            tileFeatures.tileCoord(),
            bytes,
            encoded.length,
            tileDataHash == null ? OptionalLong.empty() : OptionalLong.of(tileDataHash),
            layerStatsRows
          )
        );
      }
      // hand result off to writer
      batch.out.complete(result);
    }
  }

  private void tileWriter(Iterable<TileBatch> tileBatches) throws ExecutionException, InterruptedException {
    var f = NumberFormat.getNumberInstance(Locale.getDefault());
    f.setMaximumFractionDigits(5);

    archive.initialize(tileArchiveMetadata);
    var order = archive.tileOrder();

    TileCoord lastTile = null;
    Timer time = null;
    int currentZ = Integer.MIN_VALUE;
    try (var tileWriter = archive.newTileWriter()) {
      for (TileBatch batch : tileBatches) {
        for (var encodedTile : batch.out.get()) {
          TileCoord tileCoord = encodedTile.coord();
          assert lastTile == null ||
            order.encode(tileCoord) > order.encode(lastTile) : "Tiles out of order %s before %s"
              .formatted(lastTile, tileCoord);
          lastTile = encodedTile.coord();
          int z = tileCoord.z();
          if (z != currentZ) {
            if (time == null) {
              LOGGER.info("Starting z{}", z);
            } else {
              LOGGER.info("Finished z{} in {}, now starting z{}", currentZ, time.stop(), z);
            }
            time = Timer.start();
            currentZ = z;
          }
          tileWriter.write(encodedTile);

          stats.wroteTile(z, encodedTile.tileData().length);
          tilesByZoom[z].inc();
        }
        lastTileWritten.set(lastTile);
      }
      tileWriter.printStats();
    }

    if (time != null) {
      LOGGER.info("Finished z{} in {}", currentZ, time.stop());
    }

    archive.finish(tileArchiveMetadata);
  }

  private void printTileStats() {
    if (LOGGER.isDebugEnabled()) {
      Format format = Format.defaultInstance();
      tileStats.printStats(config.debugUrlPattern());
      LOGGER.debug(" # features: {}", format.integer(featuresProcessed.get()));
    }
  }

  private long tilesEmitted() {
    return Stream.of(tilesByZoom).mapToLong(c -> c.get()).sum();
  }

  /**
   * Generates a hash over encoded and compressed tile.
   * <p>
   * Used as an optimization to avoid writing the same (mostly ocean) tiles over and over again.
   */
  public static long generateContentHash(byte[] bytes) {
    return Hashing.fnv1a64(bytes);
  }

  /**
   * Container for a batch of tiles to be processed together in the encoder and writer threads.
   * <p>
   * The cost of encoding a tile may vary dramatically by its size (depending on the profile) so batches are sized
   * dynamically to put as little as 1 large tile, or as many as 10,000 small tiles in a batch to keep encoding threads
   * busy.
   *
   * @param in  the tile data to encode
   * @param out the future that encoder thread completes to hand finished tile off to writer thread
   */
  private record TileBatch(
    List<FeatureGroup.TileFeatures> in,
    CompletableFuture<List<TileEncodingResult>> out
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
}
