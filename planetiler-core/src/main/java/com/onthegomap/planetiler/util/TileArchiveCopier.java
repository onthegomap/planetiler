package com.onthegomap.planetiler.util;

import static com.onthegomap.planetiler.worker.Worker.joinFutures;

import com.onthegomap.planetiler.archive.ScannableTileArchive;
import com.onthegomap.planetiler.archive.TileArchiveConfig;
import com.onthegomap.planetiler.archive.TileArchiveMetadata;
import com.onthegomap.planetiler.archive.TileArchives;
import com.onthegomap.planetiler.archive.TileEncodingResult;
import com.onthegomap.planetiler.collection.IterableOnce;
import com.onthegomap.planetiler.config.Arguments;
import com.onthegomap.planetiler.config.PlanetilerConfig;
import com.onthegomap.planetiler.geo.GeoUtils;
import com.onthegomap.planetiler.geo.TileCoord;
import com.onthegomap.planetiler.geo.TileExtents;
import com.onthegomap.planetiler.geo.TileOrder;
import com.onthegomap.planetiler.stats.Counter;
import com.onthegomap.planetiler.stats.ProgressLoggers;
import com.onthegomap.planetiler.stats.Stats;
import com.onthegomap.planetiler.worker.WorkQueue;
import com.onthegomap.planetiler.worker.WorkerPipeline;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Iterator;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Predicate;
import org.roaringbitmap.RoaringBitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TileArchiveCopier {
  private static final Logger LOGGER = LoggerFactory.getLogger(TileArchiveCopier.class);

  private final PlanetilerConfig config;
  private final TileArchiveConfig dest;
  private final TileArchiveConfig source;
  private final boolean deduplicate;
  private final Stats stats;
  private final int readConcurrency;
  private final int processConcurrency;
  private final Counter.MultiThreadCounter tilesRead;
  private final Counter.MultiThreadCounter tilesWritten;
  private final AtomicReference<TileCoord> lastTileWritten = new AtomicReference<>();
  private final boolean scanSourceTileIds;
  private final TileOrder tileOrder;
  private TileArchiveMetadata metadata;

  TileArchiveCopier(
    TileArchiveConfig source,
    TileArchiveConfig dest,
    PlanetilerConfig config,
    Stats stats
  ) {
    this.source = source;
    this.dest = dest;
    this.config = config;
    this.stats = stats;
    var args = config.arguments();
    this.deduplicate = args.getBoolean("deduplicate", "attempt to deduplicate repeated tiles", true);
    this.readConcurrency = args.getInteger("read_threads", "source reader threads", Math.max(1, config.threads() / 2));
    this.scanSourceTileIds = args.getBoolean("scan_source_tiles", "scan source tile IDs before reading", false);
    this.processConcurrency = args.getInteger("process_threads", "process threads", Math.max(1, config.threads() / 2));
    this.tilesRead = stats.longCounter("tiles_read");
    this.tilesWritten = stats.longCounter("tiles_written");
    this.tileOrder = dest.tileOrder();
  }

  public static void main(String[] args) throws IOException {
    var arguments = Arguments.fromEnvOrArgs(args);
    var config = PlanetilerConfig.from(arguments);
    var stats = config.arguments().getStats();
    var source = arguments.getString("source", "source tile archive");
    var dest = arguments.getString("dest", "destination tile archive");
    copy(source, dest, config, stats);
  }

  public static void copy(
    String source,
    String dest,
    PlanetilerConfig config,
    Stats stats
  ) throws IOException {
    copy(
      TileArchiveConfig.from(source),
      TileArchiveConfig.from(dest),
      config,
      stats
    );
  }

  record TileCoords(
    @Override Iterator<Integer> iterator,
    long size
  ) implements Iterable<Integer> {}

  public static void copy(
    TileArchiveConfig source,
    TileArchiveConfig dest,
    PlanetilerConfig config,
    Stats stats
  ) {
    new TileArchiveCopier(source, dest, config, stats).run();
  }

  record ToWrite(
    TileCoord coord,
    CompletableFuture<byte[]> bytes,
    CompletableFuture<OptionalLong> hash
  ) {}

  void run() {
    try {
      dest.delete();
      var todo = enumerate();
      var pipeline = WorkerPipeline.start("copy", stats);
      WorkQueue<ToWrite> resultQueue = new WorkQueue<>("results", 100_000, 100, stats);

      var readBranch = pipeline
        .<ToWrite>fromGenerator("enumerate", next -> {
          try (resultQueue) {
            for (var coord : todo) {
              var resultHolder =
                new ToWrite(tileOrder.decode(coord), new CompletableFuture<>(), new CompletableFuture<>());
              resultQueue.accept(resultHolder);
              next.accept(resultHolder);
            }
          }
        })
        .addBuffer("to_read", 100_000, 100)
        .addWorker("read", this.readConcurrency, this::read)
        .addBuffer("to_process", 100_000, 100)
        .sinkTo("process", this.processConcurrency, this::process);

      var writeBranch = pipeline.readFromQueue(resultQueue)
        .sinkTo("write", 1, this::write);

      var loggers = ProgressLoggers.create()
        .addRatePercentCounter("read", todo.size, tilesRead::get, false)
        .addFileSize(source.getLocalPath())
        .addRateCounter("written", tilesWritten)
        .addFileSize(dest.getLocalPath())
        .newLine()
        .addProcessStats()
        .newLine()
        .addPipelineStats(readBranch)
        .addPipelineStats(writeBranch)
        .newLine()
        .add(this::getLastTileLogDetails);

      loggers.awaitAndLog(
        joinFutures(readBranch.done(), writeBranch.done()),
        config.logInterval()
      );
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }


  private TileCoords enumerate() throws IOException {
    try (var reader = TileArchives.newReader(source, config)) {
      metadata = reader.metadata();
      int minzoom = Math.max(config.minzoom(), metadata.minzoom() == null ? 0 : metadata.minzoom());
      int maxzoom = Math.min(config.maxzoom(), metadata.maxzoom() == null ? config.maxzoom() : metadata.maxzoom());
      var tileTest = config.bounds().tileExtents();
      RoaringBitmap tileIds = new RoaringBitmap();
      if (scanSourceTileIds) {
        if (reader instanceof ScannableTileArchive scannable) {
          LOGGER.info("Scanning tile IDs from {} ...", source);
          try (var iter = scannable.getAllTileCoords()) {
            while (iter.hasNext()) {
              var coord = iter.next();
              if (coord.z() >= minzoom && coord.z() <= maxzoom && tileTest.test(coord)) {
                tileIds.add(tileOrder.encode(coord));
              }
            }
          }
          tileIds.runOptimize();
          LOGGER.info("Finish scanning tile IDs from {}", source);
        } else {
          throw new IllegalArgumentException(
            "Cannot use --scan-source-tiles with an archive that is not scannable: " + source);
        }
      } else {
        LOGGER.info("Enumerating tile IDs from z{} to z{} within bounds", minzoom, maxzoom);
        Predicate<TileCoord> boundsTest = metadata.bounds() == null ?
          tileTest :
          tileTest.and(TileExtents.computeFromWorldBounds(maxzoom, GeoUtils.toWorldBounds(metadata.bounds())));
        int start = TileCoord.startIndexForZoom(minzoom);
        int end = TileCoord.endIndexForZoom(maxzoom);

        long runStart = start;
        boolean inRun = false;
        for (int i = start; i <= end; i++) {
          var coord = tileOrder.decode(i);
          if (boundsTest.test(coord)) {
            inRun = true;
          } else {
            if (inRun) {
              tileIds.add(runStart, i);
            }
            inRun = false;
            runStart = i + 1L;
          }
        }
        if (inRun) {
          tileIds.add(runStart, end);
        }
        LOGGER.info("Done enumerating tile IDs");
      }
      return new TileCoords(tileIds.iterator(), tileIds.getLongCardinality());
    }
  }

  private void read(IterableOnce<ToWrite> prev, Consumer<ToWrite> next) throws IOException {
    var readerCounter = tilesRead.counterForThread();
    try (var reader = TileArchives.newReader(source, config)) {
      for (var item : prev) {
        var tile = reader.getTile(item.coord);
        readerCounter.inc();
        item.bytes.complete(tile);
        next.accept(item);
      }
    }
  }

  private void process(IterableOnce<ToWrite> prev) throws ExecutionException, InterruptedException {
    for (var item : prev) {
      var bytes = item.bytes.get();
      item.hash.complete(
        deduplicate && bytes != null ?
          OptionalLong.of(Hashing.fnv1a64(bytes)) :
          OptionalLong.empty()
      );
    }
  }

  private void write(IterableOnce<ToWrite> prev) throws IOException, ExecutionException, InterruptedException {
    var writeCounter = tilesWritten.counterForThread();
    try (var writer = TileArchives.newWriter(dest, config)) {
      writer.initialize(metadata);
      try (var tileWriter = writer.newTileWriter()) {
        for (var item : prev) {
          var bytes = item.bytes.get();
          if (bytes != null) {
            tileWriter.write(new TileEncodingResult(
              item.coord,
              bytes,
              item.hash.get()
            ));
            lastTileWritten.set(item.coord);
            writeCounter.inc();
          }
        }
      }
      writer.finish(metadata);
    }
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
        Format.defaultInstance().percent(tileOrder.progressOnLevel(lastTile, config.bounds().tileExtents())),
        lastTile.getDebugUrl()
      );
    }
    return "last tile: " + blurb;
  }
}
