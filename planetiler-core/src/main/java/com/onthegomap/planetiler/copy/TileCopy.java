package com.onthegomap.planetiler.copy;

import static com.onthegomap.planetiler.worker.Worker.joinFutures;

import com.onthegomap.planetiler.archive.TileArchives;
import com.onthegomap.planetiler.config.Arguments;
import com.onthegomap.planetiler.stats.Counter;
import com.onthegomap.planetiler.stats.ProgressLoggers;
import com.onthegomap.planetiler.worker.WorkQueue;
import com.onthegomap.planetiler.worker.WorkerPipeline;
import java.io.IOException;
import java.time.Duration;

/**
 * Utility to copy/convert tiles and metadata from one archive into another.
 * <p>
 * Example usages:
 *
 * <pre>
 * --input=tiles.mbtiles --output=tiles.mbtiles
 * --input=tiles.mbtiles --output=tiles.pmtiles --skip_empty=false
 * --input=tiles.pmtiles --output=tiles.mbtiles
 * --input=tiles.mbtiles --output=tiles/
 * --input=tiles.mbtiles --output=tiles.json --out_tile_compression=gzip
 * --input=tiles.mbtiles --output=tiles.csv --out_tile_compression=none
 * --input=tiles.mbtiles --output=tiles.proto
 * </pre>
 */
public class TileCopy {

  private final TileCopyConfig config;

  private final Counter.MultiThreadCounter tilesReadOverall = Counter.newMultiThreadCounter();
  private final Counter.MultiThreadCounter tilesProcessedOverall = Counter.newMultiThreadCounter();
  private final Counter.MultiThreadCounter tilesWrittenOverall = Counter.newMultiThreadCounter();


  TileCopy(TileCopyConfig config) {
    this.config = config;
  }

  public void run() throws IOException {

    if (!config.inArchive().exists()) {
      throw new IllegalArgumentException("the input archive does not exist");
    }

    config.outArchive().setup(config.force(), config.append(), config.tileWriterThreads());

    final var loggers = ProgressLoggers.create()
      .addRateCounter("tiles_read", tilesReadOverall::get)
      .addRateCounter("tiles_processed", tilesProcessedOverall::get)
      .addRateCounter("tiles_written", tilesWrittenOverall::get);

    try (
      var reader = TileArchives.newReader(config.inArchive(), config.inArguments());
      var writer = TileArchives.newWriter(config.outArchive(), config.outArguments())
    ) {

      final TileCopyContext context = TileCopyContext.create(reader, writer, config);

      final var pipeline = WorkerPipeline.start("copy", config.stats());
      final WorkQueue<TileCopyWorkItem> resultQueue =
        new WorkQueue<>("results", config.workQueueCapacity(), config.workQueueMaxBatch(), config.stats());

      try (
        var it = TileCopyWorkItemGenerators.create(context)
      ) {

        writer.initialize();

        final var readerBranch = pipeline
          .<TileCopyWorkItem>fromGenerator("iterator", next -> {
            try (resultQueue) {
              while (it.hasNext()) {
                final TileCopyWorkItem t = it.next();
                resultQueue.accept(t); // put to queue immediately => retain order
                next.accept(t);
              }
            }
          })
          .addBuffer("to_read", config.workQueueCapacity(), config.workQueueMaxBatch())
          .<TileCopyWorkItem>addWorker("read", config.tileReaderThreads(), (prev, next) -> {
            final Counter tilesRead = tilesReadOverall.counterForThread();
            for (var item : prev) {
              item.loadOriginalTileData();
              tilesRead.inc();
              next.accept(item);
            }
          })
          .addBuffer("to_process", config.workQueueCapacity(), config.workQueueMaxBatch())
          .sinkTo("process", config.tileReaderThreads(), prev -> {
            final Counter tilesProcessed = tilesProcessedOverall.counterForThread();
            for (var item : prev) {
              item.process();
              tilesProcessed.inc();
            }
          });

        final var writerBranch = pipeline.readFromQueue(resultQueue)
          .sinkTo("write", config.tileWriterThreads(), prev -> {
            final Counter tilesWritten = tilesWrittenOverall.counterForThread();
            try (var tileWriter = writer.newTileWriter()) {
              for (var item : prev) {
                var result = item.toTileEncodingResult();
                if (result.tileData() == null && config.skipEmpty()) {
                  continue;
                }
                tileWriter.write(result);
                tilesWritten.inc();
              }
            }
          });

        final var writerDone = writerBranch.done().thenRun(() -> writer.finish(context.outMetadata()));
        final var readerDone = readerBranch.done();

        loggers
          .newLine()
          .addPipelineStats(readerBranch)
          .addPipelineStats(writerBranch);

        loggers.awaitAndLog(joinFutures(writerDone, readerDone), Duration.ofSeconds(1));
      }
    }
  }

  public static void main(String[] args) throws IOException {
    new TileCopy(TileCopyConfig.fromArguments(Arguments.fromEnvOrArgs(args))).run();
  }
}
