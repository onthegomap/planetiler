package com.onthegomap.planetiler.util;

import com.carrotsearch.hppc.LongIntHashMap;
import com.onthegomap.planetiler.archive.TileArchiveConfig;
import com.onthegomap.planetiler.archive.TileArchiveWriter;
import com.onthegomap.planetiler.archive.TileArchives;
import com.onthegomap.planetiler.archive.TileEncodingResult;
import com.onthegomap.planetiler.config.Arguments;
import com.onthegomap.planetiler.config.PlanetilerConfig;
import com.onthegomap.planetiler.geo.TileCoord;
import com.onthegomap.planetiler.stats.ProgressLoggers;
import com.onthegomap.planetiler.stats.Stats;
import com.onthegomap.planetiler.worker.WorkerPipeline;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.List;
import java.util.OptionalLong;
import java.util.concurrent.atomic.AtomicLong;
import vector_tile.VectorTileProto;

public class TileStats {

  public static void main(String... args) throws IOException {
    var arguments = Arguments.fromArgsOrConfigFile(args);
    var config = PlanetilerConfig.from(arguments);
    var stats = Stats.inMemory();
    var inputString = arguments.getString("input", "input file");
    var output = arguments.file("output", "output file");
    var input = TileArchiveConfig.from(inputString);
    var counter = new AtomicLong(0);
    var timer = stats.startStage("tilestats");
    record Tile(TileCoord coord, byte[] data) {}
    try (
      var reader = TileArchives.newReader(input, config);
      var result = newWriter(output)
    ) {
      var pipeline = WorkerPipeline.start("tilestats", stats)
        .<Tile>fromGenerator("enumerate", next -> {
          try (var coords = reader.getAllTileCoords()) {
            while (coords.hasNext()) {
              var coord = coords.next();
              next.accept(new Tile(coord, reader.getTile(coord)));
            }
          }
        })
        .addBuffer("coords", 10_000, 1000)
        .<TileEncodingResult>addWorker("process", config.featureProcessThreads(), (prev, next) -> {
          byte[] zipped = null;
          byte[] unzipped = null;
          VectorTileProto.Tile decoded = null;
          long hash = 0;
          List<TileEncodingResult.LayerStats> tileStats = null;

          for (var coord : prev) {
            if (!Arrays.equals(zipped, coord.data)) {
              zipped = coord.data;
              unzipped = Gzip.gunzip(coord.data);
              decoded = VectorTileProto.Tile.parseFrom(unzipped);
              hash = Hashing.fnv1a64(unzipped);
              tileStats = TileArchiveWriter.computeTileStats(decoded);
            }
            next.accept(new TileEncodingResult(coord.coord, coord.data, unzipped.length, OptionalLong.of(hash),
              tileStats));
          }
        })
        .addBuffer("results", 10_000, 1000)
        .sinkTo("write", 1, prev -> {
          writeTsvLine(result,
            "z",
            "x",
            "y",
            "hilbert",
            "tile_bytes",
            "gzipped_tile_bytes",
            "deduped_tile_id",
            "layer",
            "features",
            "layer_bytes",
            "layer_attr_bytes",
            "layer_attr_values"
          );
          LongIntHashMap ids = new LongIntHashMap();
          int num = 0;
          for (var coord : prev) {
            int id;
            if (ids.containsKey(coord.tileDataHash().getAsLong())) {
              id = ids.get(coord.tileDataHash().getAsLong());
            } else {
              ids.put(coord.tileDataHash().getAsLong(), id = ++num);
            }
            for (var layer : coord.layerStats()) {
              writeTsvLine(result,
                coord.coord().z(),
                coord.coord().x(),
                coord.coord().y(),
                coord.coord().hilbertEncoded(),
                coord.rawTileSize(),
                coord.tileData().length,
                id,
                layer.name(),
                layer.features(),
                layer.totalBytes(),
                layer.attrBytes(),
                layer.attrValues()
              );
            }
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


  private static void writeTsvLine(Writer writer, Object... values) throws IOException {
    StringBuilder builder = new StringBuilder();
    for (int i = 0; i < values.length; i++) {
      if (i > 0) {
        builder.append('\t');
      }
      builder.append(values[i]);
    }
    builder.append('\n');
    writer.write(builder.toString());
  }

  private static Writer newWriter(Path path) throws IOException {
    return new OutputStreamWriter(
      new TileArchiveWriter.FastGzipOutputStream(new BufferedOutputStream(Files.newOutputStream(path,
        StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.CREATE))));
  }
}
