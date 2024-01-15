package com.onthegomap.planetiler.util;

import com.google.common.primitives.Ints;
import com.onthegomap.planetiler.VectorTile;
import com.onthegomap.planetiler.archive.Tile;
import com.onthegomap.planetiler.archive.TileArchiveConfig;
import com.onthegomap.planetiler.archive.TileArchives;
import com.onthegomap.planetiler.archive.TileCompression;
import com.onthegomap.planetiler.config.Arguments;
import com.onthegomap.planetiler.config.PlanetilerConfig;
import com.onthegomap.planetiler.geo.GeometryException;
import com.onthegomap.planetiler.geo.GeometryType;
import com.onthegomap.planetiler.geo.TileCoord;
import com.onthegomap.planetiler.pmtiles.ReadablePmtiles;
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
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.MultiPolygon;
import org.locationtech.jts.geom.Polygon;
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
  private final boolean verbose;

  private CompareArchives(TileArchiveConfig archiveConfig1, TileArchiveConfig archiveConfig2, boolean verbose) {
    this.verbose = verbose;
    this.input1 = archiveConfig1;
    this.input2 = archiveConfig2;
  }

  public static Result compare(TileArchiveConfig archiveConfig1, TileArchiveConfig archiveConfig2,
    PlanetilerConfig config, boolean verbose) {
    return new CompareArchives(archiveConfig1, archiveConfig2, verbose).getResult(config);
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
    var verbose = arguments.getBoolean("verbose", "log each tile diff", false);
    var config = PlanetilerConfig.from(arguments);
    var input1 = TileArchiveConfig.from(inputString1);
    var input2 = TileArchiveConfig.from(inputString2);

    try {
      var result = compare(input1, input2, config, verbose);

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
    final TileCompression compression2;
    final TileCompression compression1;
    if (!input1.format().equals(input2.format())) {
      LOGGER.warn("archive1 and archive2 have different formats, got {} and {}", input1.format(), input2.format());
    }
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
      if (reader1 instanceof ReadablePmtiles pmt1 && reader2 instanceof ReadablePmtiles pmt2) {
        var header1 = pmt1.getHeader();
        var header2 = pmt2.getHeader();
        if (!Objects.equals(header1, header2)) {
          LOGGER.warn("""
            archive1 and archive2 have different pmtiles headers
            archive1: {}
            archive2: {}
            """, header1, header2);
        }
      }
      compression1 = metadata1 == null ? TileCompression.UNKNOWN : metadata1.tileCompression();
      compression2 = metadata2 == null ? TileCompression.UNKNOWN : metadata2.tileCompression();
      if (compression1 != compression2) {
        LOGGER.warn(
          "input1 and input2 must have the same compression, got {} and {} - will compare decompressed tile contents instead",
          compression1, compression2);
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }

    var order = input1.format().preferredOrder();
    var order2 = input2.format().preferredOrder();
    if (order != order2) {
      throw new IllegalArgumentException(
        "Archive orders must be the same to compare, got " + order + " and " + order2);
    }
    var stats = config.arguments().getStats();
    var total = new AtomicLong(0);
    var diffs = new AtomicLong(0);
    record Diff(Tile a, Tile b) {}
    var pipeline = WorkerPipeline.start("compare", stats)
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
      .addBuffer("diffs", 50_000, 1_000)
      .sinkTo("process", config.featureProcessThreads(), prev -> {
        boolean sameCompression = compression1 == compression2;
        for (var diff : prev) {
          var a = diff.a();
          var b = diff.b();
          total.incrementAndGet();
          if (a == null) {
            recordTileDiff(b.coord(), "archive 1 missing tile");
            diffs.incrementAndGet();
          } else if (b == null) {
            recordTileDiff(a.coord(), "archive 2 missing tile");
            diffs.incrementAndGet();
          } else if (sameCompression) {
            if (!Arrays.equals(a.bytes(), b.bytes())) {
              recordTileDiff(a.coord(), "different contents");
              diffs.incrementAndGet();
              compareTiles(
                a.coord(),
                decode(decompress(a.bytes(), compression1)),
                decode(decompress(b.bytes(), compression2))
              );
            }
          } else { // different compression
            var decompressed1 = decompress(a.bytes(), compression1);
            var decompressed2 = decompress(b.bytes(), compression2);
            if (!Arrays.equals(decompressed1, decompressed2)) {
              recordTileDiff(a.coord(), "different decompressed contents");
              diffs.incrementAndGet();
              compareTiles(
                a.coord(),
                decode(decompressed1),
                decode(decompressed2)
              );
            }
          }
        }
      });
    Format format = Format.defaultInstance();
    ProgressLoggers loggers = ProgressLoggers.create()
      .addRateCounter("tiles", total)
      .add(() -> " diffs: [ " + format.numeric(diffs, true) + " ]")
      .newLine()
      .addPipelineStats(pipeline)
      .newLine()
      .addProcessStats();
    loggers.awaitAndLog(pipeline.done(), config.logInterval());
    return new Result(total.get(), diffs.get(), diffTypes, diffsByLayer);
  }

  private void compareTiles(TileCoord coord, VectorTileProto.Tile proto1, VectorTileProto.Tile proto2) {
    compareLayerNames(coord, proto1, proto2);
    for (int i = 0; i < proto1.getLayersCount() && i < proto2.getLayersCount(); i++) {
      var layer1 = proto1.getLayers(i);
      var layer2 = proto2.getLayers(i);
      compareLayer(coord, layer1, layer2);
    }
  }

  private void compareLayer(TileCoord coord, VectorTileProto.Tile.Layer layer1, VectorTileProto.Tile.Layer layer2) {
    String name = layer1.getName();
    compareValues(coord, name, "version", layer1.getVersion(), layer2.getVersion());
    compareValues(coord, name, "extent", layer1.getExtent(), layer2.getExtent());
    compareList(coord, name, "keys list", layer1.getKeysList(), layer2.getKeysList());
    compareList(coord, name, "values list", layer1.getValuesList(), layer2.getValuesList());
    if (compareValues(coord, name, "features count", layer1.getFeaturesCount(), layer2.getFeaturesCount())) {
      var ids1 = layer1.getFeaturesList().stream().map(f -> f.getId()).toList();
      var ids2 = layer2.getFeaturesList().stream().map(f -> f.getId()).toList();
      if (compareValues(coord, name, "feature ids", Set.of(ids1), Set.of(ids2)) &&
        compareValues(coord, name, "feature order", ids1, ids2)) {
        for (int i = 0; i < layer1.getFeaturesCount() && i < layer2.getFeaturesCount(); i++) {
          var feature1 = layer1.getFeatures(i);
          var feature2 = layer2.getFeatures(i);
          compareFeature(coord, name, feature1, feature2);
        }
      }
    }
  }

  private void compareFeature(TileCoord coord, String layer, VectorTileProto.Tile.Feature feature1,
    VectorTileProto.Tile.Feature feature2) {
    compareValues(coord, layer, "feature id", feature1.getId(), feature2.getId());
    compareGeometry(coord, layer, feature1, feature2);
    compareValues(coord, layer, "feature tags", feature1.getTagsCount(), feature2.getTagsCount());
  }

  private void compareGeometry(TileCoord coord, String layer, VectorTileProto.Tile.Feature feature1,
    VectorTileProto.Tile.Feature feature2) {
    if (compareValues(coord, layer, "feature type", feature1.getType(), feature2.getType())) {
      var geomType = feature1.getType();
      if (!compareValues(coord, layer, "feature " + geomType.toString().toLowerCase() + " geometry commands",
        feature1.getGeometryList(), feature2.getGeometryList())) {
        var geom1 =
          new VectorTile.VectorGeometry(Ints.toArray(feature1.getGeometryList()), GeometryType.valueOf(geomType), 0);
        var geom2 =
          new VectorTile.VectorGeometry(Ints.toArray(feature2.getGeometryList()), GeometryType.valueOf(geomType), 0);
        try {
          compareGeometry(coord, layer, geom1.decode(), geom2.decode());
        } catch (GeometryException e) {
          LOGGER.error("Error decoding geometry", e);
        }
      }
    }
  }

  private void compareGeometry(TileCoord coord, String layer, Geometry geom1, Geometry geom2) {
    String geometryType = geom1.getGeometryType();
    compareValues(coord, layer, "feature JTS geometry type", geom1.getGeometryType(), geom2.getGeometryType());
    compareValues(coord, layer, "feature num geometries", geom1.getNumGeometries(), geom2.getNumGeometries());
    if (geom1 instanceof MultiPolygon) {
      for (int i = 0; i < geom1.getNumGeometries(); i++) {
        comparePolygon(coord, layer, geometryType, (Polygon) geom1.getGeometryN(i), (Polygon) geom2.getGeometryN(i));
      }
    } else if (geom1 instanceof Polygon p1 && geom2 instanceof Polygon p2) {
      comparePolygon(coord, layer, geometryType, p1, p2);
    }
  }

  private void comparePolygon(TileCoord coord, String layer, String geomType, Polygon p1, Polygon p2) {
    compareValues(coord, layer, geomType + " exterior ring geometry", p1.getExteriorRing(), p2.getExteriorRing());
    if (compareValues(coord, layer, geomType + " num interior rings", p1.getNumInteriorRing(),
      p2.getNumInteriorRing())) {
      for (int i = 0; i < p1.getNumInteriorRing(); i++) {
        compareValues(coord, layer, geomType + " interior ring geometry", p1.getInteriorRingN(i),
          p2.getInteriorRingN(i));
      }
    }
  }

  private void compareLayerNames(TileCoord coord, VectorTileProto.Tile proto1, VectorTileProto.Tile proto2) {
    var layers1 = proto1.getLayersList().stream().map(d -> d.getName()).toList();
    var layers2 = proto2.getLayersList().stream().map(d -> d.getName()).toList();
    compareListDetailed(coord, "tile layers", layers1, layers2);
  }

  private <T> boolean compareList(TileCoord coord, String layer, String name, List<T> value1, List<T> value2) {
    return compareValues(coord, layer, name + " unique values", Set.copyOf(value1), Set.copyOf(value2)) &&
      compareValues(coord, layer, name + " order", value1, value2);
  }

  private <T> void compareListDetailed(TileCoord coord, String name, List<T> value1, List<T> value2) {
    if (!Objects.equals(value1, value2)) {
      boolean missing = false;
      for (var layer : value1) {
        if (!value2.contains(layer)) {
          recordTileDiff(coord, name + " 2 missing " + layer);
          missing = true;
        }
      }
      for (var layer : value2) {
        if (!value1.contains(layer)) {
          recordTileDiff(coord, name + " 1 missing " + layer);
          missing = true;
        }
      }
      if (!missing) {
        recordTileDiff(coord, name + " different order");
      }
    }
  }

  private <T> boolean compareValues(TileCoord coord, String layer, String name, T value1, T value2) {
    if (!Objects.equals(value1, value2)) {
      recordLayerDiff(coord, layer, name);
      return false;
    }
    return true;
  }

  private byte[] decompress(byte[] bytes, TileCompression tileCompression) throws IOException {
    return switch (tileCompression) {
      case GZIP -> Gzip.gunzip(bytes);
      case NONE -> bytes;
      case UNKNOWN -> throw new IllegalArgumentException("Unknown compression");
    };
  }

  private VectorTileProto.Tile decode(byte[] decompressedTile) throws IOException {
    return VectorTileProto.Tile.parseFrom(decompressedTile);
  }

  private void recordLayerDiff(TileCoord coord, String layer, String issue) {
    var layerDiffs = diffsByLayer.get(layer);
    if (layerDiffs == null) {
      layerDiffs = diffsByLayer.computeIfAbsent(layer, k -> new ConcurrentHashMap<>());
    }
    layerDiffs.merge(issue, 1L, Long::sum);
    if (verbose) {
      LOGGER.debug("{} layer {} {}", coord, layer, issue);
    }
  }

  private void recordTileDiff(TileCoord coord, String issue) {
    diffTypes.merge(issue, 1L, Long::sum);
    if (verbose) {
      LOGGER.debug("{} {}", coord, issue);
    }
  }

  public record Result(
    long total, long tileDiffs, Map<String, Long> diffTypes,
    Map<String, Map<String, Long>> diffsByLayer
  ) {}
}
