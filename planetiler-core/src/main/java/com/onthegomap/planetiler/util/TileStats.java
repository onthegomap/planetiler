package com.onthegomap.planetiler.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.onthegomap.planetiler.archive.Tile;
import com.onthegomap.planetiler.archive.TileArchiveConfig;
import com.onthegomap.planetiler.archive.TileArchiveWriter;
import com.onthegomap.planetiler.archive.TileArchives;
import com.onthegomap.planetiler.archive.TileEncodingResult;
import com.onthegomap.planetiler.config.Arguments;
import com.onthegomap.planetiler.config.PlanetilerConfig;
import com.onthegomap.planetiler.stats.ProgressLoggers;
import com.onthegomap.planetiler.stats.Stats;
import com.onthegomap.planetiler.worker.WorkerPipeline;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import vector_tile.VectorTileProto;

public class TileStats {
  private static final ObjectMapper mapper = new ObjectMapper();

  public static void main(String... args) throws IOException {
    var arguments = Arguments.fromArgsOrConfigFile(args);
    var config = PlanetilerConfig.from(arguments);
    var stats = Stats.inMemory();
    var inputString = arguments.getString("input", "input file");
    var output = arguments.file("output", "output file");
    var input = TileArchiveConfig.from(inputString);
    var counter = new AtomicLong(0);
    var timer = stats.startStage("tilestats");
    try (
      var reader = TileArchives.newReader(input, config);
      var result = newWriter(output);
      //      var jsonWriter = mapper.writer()
      //        .withRootValueSeparator("\n")
      //        .writeValues(result)
    ) {
      var pipeline = WorkerPipeline.start("tilestats", stats)
        .<Tile>fromGenerator("enumerate", next -> {
          try (var tiles = reader.getAllTiles()) {
            while (tiles.hasNext()) {
              next.accept(tiles.next());
            }
          }
        })
        .addBuffer("coords", 10_000, 1000)
        .<byte[]>addWorker("process", config.featureProcessThreads(), (prev, next) -> {
          byte[] zipped = null;
          byte[] unzipped = null;
          VectorTileProto.Tile decoded;
          List<TileEncodingResult.LayerStats> tileStats = null;

          for (var coord : prev) {
            if (!Arrays.equals(zipped, coord.bytes())) {
              zipped = coord.bytes();
              unzipped = Gzip.gunzip(coord.bytes());
              decoded = VectorTileProto.Tile.parseFrom(unzipped);
              tileStats = TileArchiveWriter.computeTileStats(decoded);
            }
            next.accept(mapper.writeValueAsBytes(new OutputTileStats(
              coord.coord().z(),
              coord.coord().x(),
              coord.coord().y(),
              coord.coord().hilbertEncoded(),
              unzipped.length,
              zipped.length,
              tileStats
            )));
          }
        })
        .addBuffer("results", 10_000, 1000)
        .sinkTo("write", 1, prev -> {
          byte[] newLine = "\n".getBytes(StandardCharsets.UTF_8);
          for (var coord : prev) {
            result.write(coord);
            result.write(newLine);
            counter.incrementAndGet();
          }
        });
      ProgressLoggers loggers = ProgressLoggers.create()
        .addRateCounter("tiles", counter)
        .newLine()
        .addPipelineStats(pipeline)
        .newLine()
        .addProcessStats();
      pipeline.awaitAndLog(loggers, config.logInterval());

      timer.stop();
      stats.printSummary();
    }
  }

  record OutputTileStats(
    int z,
    int x,
    int y,
    int hilbert,
    int total_bytes,
    int gzipped_bytes,
    List<TileEncodingResult.LayerStats> layers
  ) {}

  //
  //  private static void writeJsonLine(Writer writer, OutputTileStats stats) throws IOException {
  //    mapper.writeValue(writer, stats);
  //    StringBuilder builder = new StringBuilder();
  //    for (int i = 0; i < values.length; i++) {
  //      if (i > 0) {
  //        builder.append('\t');
  //      }
  //      builder.append(values[i]);
  //    }
  //    builder.append('\n');
  //    writer.write(builder.toString());
  //  }

  private static OutputStream newWriter(Path path) throws IOException {
    return new TileArchiveWriter.FastGzipOutputStream(new BufferedOutputStream(Files.newOutputStream(path,
      StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.CREATE)));
  }
}
