package com.onthegomap.planetiler.util;

import static com.onthegomap.planetiler.worker.Worker.joinFutures;

import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import com.onthegomap.planetiler.VectorTile;
import com.onthegomap.planetiler.archive.Tile;
import com.onthegomap.planetiler.archive.TileArchiveConfig;
import com.onthegomap.planetiler.archive.TileArchives;
import com.onthegomap.planetiler.config.Arguments;
import com.onthegomap.planetiler.config.PlanetilerConfig;
import com.onthegomap.planetiler.geo.TileCoord;
import com.onthegomap.planetiler.stats.ProgressLoggers;
import com.onthegomap.planetiler.stats.Stats;
import com.onthegomap.planetiler.worker.WorkQueue;
import com.onthegomap.planetiler.worker.WorkerPipeline;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import vector_tile.VectorTileProto;

/**
 * Utilities for extracting tile and layer size summaries from encoded vector tiles.
 * <p>
 * {@link #computeTileStats(VectorTileProto.Tile)} extracts statistics about each layer in a tile and
 * {@link #formatOutputRows(TileCoord, int, List)} formats them as row of a TSV file to write.
 * <p>
 * To generate a tsv.gz file with stats for each tile, you can add {@code --output-layerstats} option when generating an
 * archive, or run the following an existing archive:
 *
 * <pre>
 * {@code
 * java -jar planetiler.jar stats --input=<path to pmtiles or mbtiles> --output=layerstats.tsv.gz
 * }
 * </pre>
 */
public class TileSizeStats {

  private static final int BATCH_SIZE = 1_000;
  private static final CsvMapper MAPPER = new CsvMapper();
  private static final CsvSchema SCHEMA = MAPPER
    .schemaFor(OutputRow.class)
    .withoutHeader()
    .withColumnSeparator('\t')
    .withLineSeparator("\n");
  private static final ObjectWriter WRITER = MAPPER.writer(SCHEMA);

  /** Returns the default path that a layerstats file should go relative to an existing archive. */
  public static Path getDefaultLayerstatsPath(Path archive) {
    return archive.resolveSibling(archive.getFileName() + ".layerstats.tsv.gz");
  }

  public static void main(String... args) {
    var arguments = Arguments.fromArgsOrConfigFile(args);
    var config = PlanetilerConfig.from(arguments);
    var stats = Stats.inMemory();
    var download = arguments.getBoolean("download_osm_tile_weights|download", "download OSM tile weights file", false);
    if (download && !Files.exists(config.tileWeights())) {
      TopOsmTiles.downloadPrecomputed(config);
    }
    var tileStats = new TilesetSummaryStatistics(TileWeights.readFromFile(config.tileWeights()));
    var inputString = arguments.getString("input", "input file");
    var input = TileArchiveConfig.from(inputString);
    var localPath = input.getLocalPath();
    var output = localPath == null ?
      arguments.file("output", "output file") :
      arguments.file("output", "output file", getDefaultLayerstatsPath(localPath));
    var counter = new AtomicLong(0);
    var timer = stats.startStage("tilestats");
    record Batch(List<Tile> tiles, CompletableFuture<List<String>> stats) {}
    WorkQueue<Batch> writerQueue = new WorkQueue<>("tilestats_write_queue", 1_000, 1, stats);
    var pipeline = WorkerPipeline.start("tilestats", stats);
    var readBranch = pipeline
      .<Batch>fromGenerator("enumerate", next -> {
        try (
          var reader = TileArchives.newReader(input, config);
          var tiles = reader.getAllTiles();
          writerQueue
        ) {
          var writer = writerQueue.threadLocalWriter();
          List<Tile> batch = new ArrayList<>(BATCH_SIZE);
          while (tiles.hasNext()) {
            var tile = tiles.next();
            if (batch.size() >= BATCH_SIZE) {
              var result = new Batch(batch, new CompletableFuture<>());
              writer.accept(result);
              next.accept(result);
              batch = new ArrayList<>(BATCH_SIZE);
            }
            batch.add(tile);
            counter.incrementAndGet();
          }
          if (!batch.isEmpty()) {
            var result = new Batch(batch, new CompletableFuture<>());
            writer.accept(result);
            next.accept(result);
          }
        }
      })
      .addBuffer("coords", 1_000)
      .sinkTo("process", config.featureProcessThreads(), prev -> {
        byte[] zipped = null;
        byte[] unzipped;
        VectorTileProto.Tile decoded;
        List<LayerStats> layerStats = null;

        var updater = tileStats.threadLocalUpdater();
        for (var batch : prev) {
          List<String> lines = new ArrayList<>(batch.tiles.size());
          for (var tile : batch.tiles) {
            if (!Arrays.equals(zipped, tile.bytes())) {
              zipped = tile.bytes();
              unzipped = Gzip.gunzip(tile.bytes());
              decoded = VectorTileProto.Tile.parseFrom(unzipped);
              layerStats = computeTileStats(decoded);
            }
            updater.recordTile(tile.coord(), zipped.length, layerStats);
            lines.addAll(TileSizeStats.formatOutputRows(tile.coord(), zipped.length, layerStats));
          }
          batch.stats.complete(lines);
        }
      });

    var writeBranch = pipeline.readFromQueue(writerQueue)
      .sinkTo("write", 1, prev -> {
        try (var writer = newWriter(output)) {
          writer.write(headerRow());
          for (var batch : prev) {
            for (var line : batch.stats.get()) {
              writer.write(line);
            }
          }
        }
      });
    ProgressLoggers loggers = ProgressLoggers.create()
      .addRateCounter("tiles", counter)
      .newLine()
      .addPipelineStats(readBranch)
      .addPipelineStats(writeBranch)
      .newLine()
      .addProcessStats();
    loggers.awaitAndLog(joinFutures(readBranch.done(), writeBranch.done()), config.logInterval());

    timer.stop();
    tileStats.printStats(config.debugUrlPattern());
    stats.printSummary();
  }

  /** Returns the TSV rows to output for all the layers in a tile. */
  public static List<String> formatOutputRows(TileCoord tileCoord, int archivedBytes, List<LayerStats> layerStats)
    throws IOException {
    int hilbert = tileCoord.hilbertEncoded();
    List<String> result = new ArrayList<>(layerStats.size());
    for (var layer : layerStats) {
      result.add(lineToString(new OutputRow(
        tileCoord.z(),
        tileCoord.x(),
        tileCoord.y(),
        hilbert,
        archivedBytes,
        layer.layer,
        layer.layerBytes,
        layer.layerFeatures,
        layer.layerGeometries,
        layer.layerAttrBytes,
        layer.layerAttrKeys,
        layer.layerAttrValues
      )));
    }
    return result;
  }

  /**
   * Opens a new gzip (level 1/fast) writer to {@code path}, creating a new one or replacing an existing file at that
   * path.
   */
  public static Writer newWriter(Path path) throws IOException {
    return new OutputStreamWriter(
      new FastGzipOutputStream(new BufferedOutputStream(Files.newOutputStream(path,
        StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.CREATE))));
  }

  /** Returns {@code output} encoded as a TSV row string. */
  public static String lineToString(OutputRow output) throws IOException {
    return WRITER.writeValueAsString(output);
  }

  /** Returns the header row for the output TSV file. */
  public static String headerRow() {
    return String.join(
      String.valueOf(SCHEMA.getColumnSeparator()),
      SCHEMA.getColumnNames()
    ) + new String(SCHEMA.getLineSeparator());
  }

  /** Returns the size and statistics for each layer in {@code proto}. */
  public static List<LayerStats> computeTileStats(VectorTileProto.Tile proto) {
    if (proto == null) {
      return List.of();
    }
    List<LayerStats> result = new ArrayList<>(proto.getLayersCount());
    for (var layer : proto.getLayersList()) {
      int attrSize = 0;
      for (var key : layer.getKeysList().asByteStringList()) {
        attrSize += key.size();
      }
      for (var value : layer.getValuesList()) {
        attrSize += value.getSerializedSize();
      }
      int geomCount = 0;
      for (var feature : layer.getFeaturesList()) {
        geomCount += VectorTile.countGeometries(feature);
      }
      result.add(new LayerStats(
        layer.getName(),
        layer.getSerializedSize(),
        layer.getFeaturesCount(),
        geomCount,
        attrSize,
        layer.getKeysCount(),
        layer.getValuesCount()
      ));
    }
    result.sort(Comparator.naturalOrder());
    return result;
  }

  /** Model for the data contained in each row in the TSV. */
  @JsonPropertyOrder({
    "z",
    "x",
    "y",
    "hilbert",
    "archived_tile_bytes",
    "layer",
    "layer_bytes",
    "layer_features",
    "layer_geometries",
    "layer_attr_bytes",
    "layer_attr_keys",
    "layer_attr_values"
  })
  @JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
  public record OutputRow(
    int z,
    int x,
    int y,
    int hilbert,
    int archivedTileBytes,
    String layer,
    int layerBytes,
    int layerFeatures,
    int layerGeometries,
    int layerAttrBytes,
    int layerAttrKeys,
    int layerAttrValues
  ) {}

  /** Stats extracted from a layer in a vector tile. */
  public record LayerStats(
    String layer,
    int layerBytes,
    int layerFeatures,
    int layerGeometries,
    int layerAttrBytes,
    int layerAttrKeys,
    int layerAttrValues
  ) implements Comparable<LayerStats> {

    @Override
    public int compareTo(LayerStats o) {
      return layer.compareTo(o.layer);
    }
  }
}
