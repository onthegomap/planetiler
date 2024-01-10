package com.onthegomap.planetiler.util;

import com.onthegomap.planetiler.archive.Tile;
import com.onthegomap.planetiler.archive.TileArchiveConfig;
import com.onthegomap.planetiler.archive.TileArchives;
import com.onthegomap.planetiler.archive.TileCompression;
import com.onthegomap.planetiler.config.Arguments;
import com.onthegomap.planetiler.config.PlanetilerConfig;
import com.onthegomap.planetiler.stats.ProgressLoggers;
import com.onthegomap.planetiler.worker.WorkerPipeline;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import vector_tile.VectorTileProto;

/**
 * Compares the contents of two tile archives.
 * <p>
 * To run:
 *
 * <pre>{@code
 * java -jar planetiler.jar compare [options] {path/to/archive1} {path/to/archive2}
 * }</pre>
 */
public class CompareArchives {

  private static final Logger LOGGER = LoggerFactory.getLogger(CompareArchives.class);
  private final Map<String, Long> diffTypes = new ConcurrentHashMap<>();
  private final Map<String, Map<String, Long>> diffsByLayer = new ConcurrentHashMap<>();
  private final TileArchiveConfig input1;
  private final TileArchiveConfig input2;

  private CompareArchives(TileArchiveConfig archiveConfig1, TileArchiveConfig archiveConfig2) {
    this.input1 = archiveConfig1;
    this.input2 = archiveConfig2;
  }

  public static Result compare(TileArchiveConfig archiveConfig1, TileArchiveConfig archiveConfig2,
    PlanetilerConfig config) {
    return new CompareArchives(archiveConfig1, archiveConfig2).getResult(config);
  }

  public static void main(String[] args) {
    if (args.length < 2) {
      System.err.println("Usage: compare [options] {path/to/archive1} {path/to/archive2}");
      System.exit(1);
    }
    // last 2 args are paths to the archives
    String inputString1 = args[args.length - 2];
    String inputString2 = args[args.length - 1];
    var arguments = Arguments.fromArgsOrConfigFile(Arrays.copyOf(args, args.length - 2));
    var config = PlanetilerConfig.from(arguments);
    var input1 = TileArchiveConfig.from(inputString1);
    var input2 = TileArchiveConfig.from(inputString2);

    try {
      var result = compare(input1, input2, config);

      var format = Format.defaultInstance();
      if (LOGGER.isInfoEnabled()) {
        LOGGER.info("Detailed diffs:");
        for (var entry : result.diffsByLayer.entrySet()) {
          LOGGER.info("  \"{}\" layer", entry.getKey());
          for (var layerEntry : entry.getValue().entrySet().stream().sorted(Map.Entry.comparingByValue()).toList()) {
            LOGGER.info("    {}: {}", layerEntry.getKey(), format.integer(layerEntry.getValue()));
          }
        }
        for (var entry : result.diffTypes.entrySet().stream().sorted(Map.Entry.comparingByValue()).toList()) {
          LOGGER.info("  {}: {}", entry.getKey(), format.integer(entry.getValue()));
        }
        LOGGER.info("Total tiles: {}", format.integer(result.total));
        LOGGER.info("Total diffs: {} ({} of all tiles)", format.integer(result.tileDiffs),
          format.percent(result.tileDiffs * 1d / result.total));
      }
    } catch (IllegalArgumentException e) {
      LOGGER.error("Error comparing archives {}", e.getMessage());
      System.exit(1);
    }
  }

  private Result getResult(PlanetilerConfig config) {
    if (!input1.format().equals(input2.format())) {
      throw new IllegalArgumentException(
        "input1 and input2 must have the same format, got " + input1.format() + " and " +
          input2.format());
    }
    final TileCompression compression;
    try (
      var reader1 = TileArchives.newReader(input1, config);
      var reader2 = TileArchives.newReader(input2, config);
    ) {
      var metadata1 = reader1.metadata();
      var metadata2 = reader2.metadata();
      if (!Objects.equals(metadata1, metadata2)) {
        LOGGER.warn("""
          archive1 and archive2 have different metadata
          archive1: {}
          archive2: {}
          """, reader1.metadata(), reader2.metadata());
      }
      compression = metadata1 == null ? TileCompression.UNKNWON : metadata1.tileCompression();
      TileCompression compression2 = metadata2 == null ? TileCompression.UNKNWON : metadata2.tileCompression();
      if (compression != compression2) {
        throw new IllegalArgumentException(
          "input1 and input2 must have the same compression, got " + compression + " and " +
            compression2);
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }

    var order = input1.format().preferredOrder();
    var stats = config.arguments().getStats();
    var total = new AtomicLong(0);
    var diffs = new AtomicLong(0);
    record Diff(Tile a, Tile b) {}
    var pipeline = WorkerPipeline.start("tilestats", stats)
      .<Diff>fromGenerator("enumerate", next -> {
        try (
          var reader1 = TileArchives.newReader(input1, config);
          var tiles1 = reader1.getAllTiles();
          var reader2 = TileArchives.newReader(input2, config);
          var tiles2 = reader2.getAllTiles()
        ) {
          Supplier<Tile> supplier1 = () -> tiles1.hasNext() ? tiles1.next() : null;
          Supplier<Tile> supplier2 = () -> tiles2.hasNext() ? tiles2.next() : null;
          var tile1 = supplier1.get();
          var tile2 = supplier2.get();
          while (tile1 != null || tile2 != null) {
            if (tile1 == null) {
              next.accept(new Diff(null, tile2));
              tile2 = supplier2.get();
            } else if (tile2 == null) {
              next.accept(new Diff(tile1, null));
              tile1 = supplier1.get();
            } else {
              if (tile1.coord().equals(tile2.coord())) {
                next.accept(new Diff(tile1, tile2));
                tile1 = supplier1.get();
                tile2 = supplier2.get();
              } else if (order.encode(tile1.coord()) < order.encode(tile2.coord())) {
                next.accept(new Diff(tile1, null));
                tile1 = supplier1.get();
              } else {
                next.accept(new Diff(null, tile2));
                tile2 = supplier2.get();
              }
            }
          }
        }
      })
      .addBuffer("diffs", 1_000)
      .sinkTo("process", config.featureProcessThreads(), prev -> {
        for (var diff : prev) {
          var a = diff.a();
          var b = diff.b();
          total.incrementAndGet();
          if (a == null) {
            recordTileDiff("archive 1 missing tile");
            diffs.incrementAndGet();
          } else if (b == null) {
            recordTileDiff("archive 2 missing tile");
            diffs.incrementAndGet();
          } else if (!Arrays.equals(a.bytes(), b.bytes())) {
            recordTileDiff("different contents");
            diffs.incrementAndGet();
            var proto1 = decode(a.bytes(), compression);
            var proto2 = decode(b.bytes(), compression);
            compareTiles(proto1, proto2);
          }
        }
      });
    ProgressLoggers loggers = ProgressLoggers.create()
      .addRateCounter("tiles", total)
      .newLine()
      .addPipelineStats(pipeline)
      .newLine()
      .addProcessStats();
    loggers.awaitAndLog(pipeline.done(), config.logInterval());
    return new Result(total.get(), diffs.get(), diffTypes, diffsByLayer);
  }

  private void compareTiles(VectorTileProto.Tile proto1, VectorTileProto.Tile proto2) {
    compareLayerNames(proto1, proto2);
    for (int i = 0; i < proto1.getLayersCount() && i < proto2.getLayersCount(); i++) {
      var layer1 = proto1.getLayers(i);
      var layer2 = proto2.getLayers(i);
      compareLayer(layer1, layer2);
    }
  }

  private void compareLayer(VectorTileProto.Tile.Layer layer1, VectorTileProto.Tile.Layer layer2) {
    String name = layer1.getName();
    compareValues(name, "version", layer1.getVersion(), layer2.getVersion());
    compareValues(name, "extent", layer1.getExtent(), layer2.getExtent());
    compareList(name, "keys list", layer1.getKeysList(), layer2.getKeysList());
    compareList(name, "values list", layer1.getValuesList(), layer2.getValuesList());
    if (compareValues(name, "features count", layer1.getFeaturesCount(), layer2.getFeaturesCount())) {
      var ids1 = layer1.getFeaturesList().stream().map(f -> f.getId());
      var ids2 = layer1.getFeaturesList().stream().map(f -> f.getId());
      if (compareValues(name, "feature ids", Set.of(ids1), Set.of(ids2)) &&
        compareValues(name, "feature order", ids1, ids2)) {
        for (int i = 0; i < layer1.getFeaturesCount() && i < layer2.getFeaturesCount(); i++) {
          var feature1 = layer1.getFeatures(i);
          var feature2 = layer2.getFeatures(i);
          compareFeature(name, feature1, feature2);
        }
      }
    }
  }

  private void compareFeature(String layer, VectorTileProto.Tile.Feature feature1,
    VectorTileProto.Tile.Feature feature2) {
    compareValues(layer, "feature id", feature1.getId(), feature2.getId());
    compareValues(layer, "feature type", feature1.getType(), feature2.getType());
    compareValues(layer, "feature geometry", feature1.getGeometryList(), feature2.getGeometryList());
    compareValues(layer, "feature tags", feature1.getTagsCount(), feature2.getTagsCount());
  }

  private void compareLayerNames(VectorTileProto.Tile proto1, VectorTileProto.Tile proto2) {
    var layers1 = proto1.getLayersList().stream().map(d -> d.getName()).toList();
    var layers2 = proto2.getLayersList().stream().map(d -> d.getName()).toList();
    compareListDetailed("tile layers", layers1, layers2);
  }

  private <T> boolean compareList(String layer, String name, List<T> value1, List<T> value2) {
    return compareValues(layer, name + " unique values", Set.copyOf(value1), Set.copyOf(value2)) &&
      compareValues(layer, name + " order", value1, value2);
  }

  private <T> void compareListDetailed(String name, List<T> value1, List<T> value2) {
    if (!Objects.equals(value1, value2)) {
      boolean missing = false;
      for (var layer : value1) {
        if (!value2.contains(layer)) {
          recordTileDiff(name + " 2 missing " + layer);
          missing = true;
        }
      }
      for (var layer : value2) {
        if (!value1.contains(layer)) {
          recordTileDiff(name + " 1 missing " + layer);
          missing = true;
        }
      }
      if (!missing) {
        recordTileDiff(name + " different order");
      }
    }
  }

  private <T> boolean compareValues(String layer, String name, T value1, T value2) {
    if (!Objects.equals(value1, value2)) {
      recordLayerDiff(layer, name);
      return false;
    }
    return true;
  }

  private VectorTileProto.Tile decode(byte[] bytes, TileCompression tileCompression) throws IOException {
    byte[] decompressed = switch (tileCompression) {
      case GZIP -> Gzip.gunzip(bytes);
      case NONE -> bytes;
      case UNKNWON -> throw new IllegalArgumentException("Unknown compression");
    };
    return VectorTileProto.Tile.parseFrom(decompressed);
  }

  private void recordLayerDiff(String layer, String issue) {
    var layerDiffs = diffsByLayer.get(layer);
    if (layerDiffs == null) {
      layerDiffs = diffsByLayer.computeIfAbsent(layer, k -> new ConcurrentHashMap<>());
    }
    layerDiffs.merge(issue, 1L, Long::sum);
  }

  private void recordTileDiff(String issue) {
    diffTypes.merge(issue, 1L, Long::sum);
  }

  public record Result(long total, long tileDiffs, Map<String, Long> diffTypes,
    Map<String, Map<String, Long>> diffsByLayer) {}
}
