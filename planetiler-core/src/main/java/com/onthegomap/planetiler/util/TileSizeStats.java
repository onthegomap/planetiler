package com.onthegomap.planetiler.util;

import static com.onthegomap.planetiler.worker.Worker.joinFutures;

import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import com.google.common.io.CountingInputStream;
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
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.lang3.tuple.Pair;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryCollection;
import org.maplibre.mlt.converter.encodings.MltTypeMap;
import org.maplibre.mlt.converter.mvt.MapboxVectorTile;
import org.maplibre.mlt.data.Feature;
import org.maplibre.mlt.decoder.DecodingUtils;
import org.maplibre.mlt.decoder.MltDecoder;
import org.maplibre.mlt.metadata.tileset.MltTilesetMetadata;
import vector_tile.VectorTileProto;

/**
 * Utilities for extracting tile and layer size summaries from encoded vector tiles.
 * <p>
 * {@link #computeTileStats(VectorTileProto.Tile)} extracts statistics about each layer in a tile and
 * {@link TsvSerializer} formats them as row of a TSV file to write.
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
  private static final CsvSchema SCHEMA = new CsvMapper()
    .schemaFor(OutputRow.class)
    .withoutHeader()
    .withColumnSeparator('\t')
    .withLineSeparator("\n");

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
        var layerStatsSerializer = TileSizeStats.newThreadLocalSerializer();
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
            lines.addAll(layerStatsSerializer.formatOutputRows(tile.coord(), zipped.length, layerStats));
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

  /** Returns a {@link TsvSerializer} that can be used by a single thread to convert to CSV rows. */
  public static TsvSerializer newThreadLocalSerializer() {
    // CsvMapper is not entirely thread safe, and can end up with a BufferRecycler memory leak when writeValueAsString
    // is called billions of times from multiple threads, so we generate a new instance per serializing thread
    ObjectWriter writer = new CsvMapper().writer(SCHEMA);
    return (tileCoord, archivedBytes, layerStats) -> {
      int hilbert = tileCoord.hilbertEncoded();
      List<String> result = new ArrayList<>(layerStats.size());
      for (var layer : layerStats) {
        result.add(writer.writeValueAsString(new OutputRow(
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
    };
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

  public static List<LayerStats> computeMltTileStats(VectorTile tile, MapboxVectorTile input, byte[] output) {
    Map<String, Integer> encodedLayerSizes = new HashMap<>();
    Map<String, Integer> encodedLayerAttributeSizes = new HashMap<>();
    try (final var stream = new ByteArrayInputStream(output)) {
      while (stream.available() > 0) {
        int length = DecodingUtils.decodeVarint(stream);
        Pair<Integer, Integer> tag = DecodingUtils.decodeVarintWithLength(stream);
        int bodySize = length - tag.getRight();
        int attrBytes = 0;
        if (tag.getLeft() == 1) {
          try (var countStream = new CountingInputStream(stream)) {
            final var metadataExtent = MltDecoder.parseEmbeddedMetadata(countStream);
            MltTilesetMetadata.FeatureTableSchema metadata = metadataExtent.getLeft();
            countStream.skipNBytes(bodySize - countStream.getCount());
            for (var column : metadata.getColumnsList()) {
              if (!MltTypeMap.Tag0x01.isGeometry(column) && !MltTypeMap.Tag0x01.isID(column)) {
                attrBytes += column.getSerializedSize();
              }
            }
            encodedLayerSizes.put(metadata.getName(), length);
            encodedLayerAttributeSizes.put(metadata.getName(), attrBytes);
          }
        } else {
          // Skip the remainder of this one
          stream.skipNBytes((long) length - tag.getRight());
        }
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return input.layers().stream().map(layer -> new LayerStats(
      layer.name(),
      encodedLayerSizes.getOrDefault(layer.name(), -1),
      layer.features().size(),
      countGeometries(layer.features()),
      encodedLayerAttributeSizes.getOrDefault(layer.name(), -1),
      tile.getNumKeys(layer.name()),
      tile.getNumValues(layer.name())
    )).toList();
  }

  private static int countGeometries(List<Feature> features) {
    return features.stream().mapToInt(feature -> countGeometries(feature.geometry())).sum();
  }

  private static int countGeometries(Geometry geometry) {
    if (geometry instanceof GeometryCollection gc) {
      int num = 0;
      for (int i = 0; i < gc.getNumGeometries(); i++) {
        num += countGeometries(gc.getGeometryN(i));
      }
      return num;
    } else {
      return 1;
    }
  }

  @FunctionalInterface
  public interface TsvSerializer {

    /** Returns the TSV rows to output for all the layers in a tile. */
    List<String> formatOutputRows(TileCoord tileCoord, int archivedBytes, List<LayerStats> layerStats)
      throws IOException;
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
