package com.onthegomap.planetiler.util;

import com.onthegomap.planetiler.config.PlanetilerConfig;
import com.onthegomap.planetiler.geo.TileCoord;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.LongSummaryStatistics;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility that computes min/max/average sizes for each vector tile layers at each zoom level, then computes combined
 * summary statistics at the end.
 * <p>
 * Provide a {@link TileWeights} instance to compute weighted average tile sizes based on actual tile traffic.
 */
public class TilesetSummaryStatistics {

  private static final int TOP_N_TILES = 10;
  private static final int WARN_BYTES = 100_000;
  private static final int ERROR_BYTES = 500_000;
  private static final Logger LOGGER = LoggerFactory.getLogger(TilesetSummaryStatistics.class);
  private final TileWeights tileWeights;

  // instead of threads updating concurrent data structures, each thread gets a thread-local
  // Summary instance it can update without contention that are combined at the end.
  private final List<Summary> summaries = new CopyOnWriteArrayList<>();

  public TilesetSummaryStatistics(TileWeights tileWeights) {
    this.tileWeights = tileWeights;
  }

  public TilesetSummaryStatistics() {
    this(new TileWeights());
  }

  private static String tileBiggestLayers(Format formatter, TileSummary tile) {
    int minSize = tile.layers.stream().mapToInt(l -> l.layerBytes()).max().orElse(0);
    return tile.layers.stream()
      .filter(d -> d.layerBytes() >= minSize)
      .sorted(Comparator.comparingInt(d -> -d.layerBytes()))
      .map(d -> d.layer() + ":" + formatter.storage(d.layerBytes()))
      .collect(Collectors.joining(", "));
  }

  /** Returns a combined {@link Summary} from each thread's {@link Updater}. */
  public Summary summary() {
    return summaries.stream().reduce(new Summary(), Summary::mergeIn);
  }

  /** Logs biggest tiles, max layer size by zoom, and weighted average tile sizes. */
  @SuppressWarnings("java:S2629")
  public void printStats(String debugUrlPattern) {
    LOGGER.debug("Tile stats:");
    Summary result = summary();
    var overallStats = result.get();
    var formatter = Format.defaultInstance();
    LOGGER.debug("Biggest tiles (gzipped)\n{}", overallStats.formatBiggestTiles(debugUrlPattern));
    var alreadyListed = overallStats.biggestTiles().stream()
      .map(TileSummary::coord)
      .collect(Collectors.toSet());
    var otherTiles = result.layers().stream()
      .flatMap(layer -> result.get(layer).biggestTiles().stream().limit(1))
      .filter(tile -> !alreadyListed.contains(tile.coord) && tile.archivedSize > WARN_BYTES)
      .toList();
    if (!otherTiles.isEmpty()) {
      LOGGER.info("Other tiles with large layers\n{}",
        otherTiles.stream()
          .map(tile -> "%d/%d/%d (%s) %s (%s)".formatted(
            tile.coord.z(),
            tile.coord.x(),
            tile.coord.y(),
            formatter.storage(tile.archivedSize),
            tile.coord.getDebugUrl(debugUrlPattern),
            tileBiggestLayers(formatter, tile)
          )).collect(Collectors.joining("\n")));
    }

    LOGGER.debug("Max tile sizes\n{}\n{}\n{}",
      result.formatTable(n -> {
        String string = " " + formatter.storage(n, true);
        return n.intValue() > ERROR_BYTES ? AnsiColors.red(string) :
          n.intValue() > WARN_BYTES ? AnsiColors.yellow(string) :
          string;
      }, Cell::maxSize),
      result.formatRow("full tile",
        formatter::storage,
        z -> result.get(z).maxSize(),
        result.get().maxSize()
      ),
      result.formatRow("gzipped",
        formatter::storage,
        z -> result.get(z).maxArchivedSize(),
        result.get().maxArchivedSize()
      )
    );
    LOGGER.debug("   Max tile: {} (gzipped: {})",
      formatter.storage(overallStats.maxSize()),
      formatter.storage(overallStats.maxArchivedSize()));
    LOGGER.debug("   Avg tile: {} (gzipped: {}) {}",
      formatter.storage(overallStats.weightedAverageSize()),
      formatter.storage(overallStats.weightedAverageArchivedSize()),
      overallStats.totalWeight <= 0 ?
        "no tile weights, use --download-osm-tile-weights for weighted average" :
        "using weighted average based on OSM traffic");
    LOGGER.debug("    # tiles: {}", formatter.integer(overallStats.numTiles()));
  }

  /**
   * Returns an {@link Updater} that accepts individual tile layer stats from a thread that will eventually be combined
   * into the final tileset report.
   */
  public Updater threadLocalUpdater() {
    return new Updater();
  }

  /** Aggregated statistics for a layer/zoom, layer, zoom, or entire tileset. */
  public static class Cell {
    private final LongSummaryStatistics archivedBytes = new LongSummaryStatistics();
    private final LongSummaryStatistics bytes = new LongSummaryStatistics();
    private final PriorityQueue<TileSummary> topTiles = new PriorityQueue<>();
    private long weightedBytesSum;
    private long weightedArchivedBytesSum;
    private long totalWeight;
    private int bigTileCutoff = 0;

    private static Cell combine(Cell a, Cell b) {
      return new Cell().mergeIn(a).mergeIn(b);
    }

    /** Max raw layer bytes (or tile size when aggregated over all layers). */
    public long maxSize() {
      return Math.max(0, bytes.getMax());
    }

    /** Max gzipped tile bytes (or 0 when broken-out by layer). */
    public long maxArchivedSize() {
      return Math.max(0, archivedBytes.getMax());
    }

    /** Total tiles included in this aggregation. */
    public long numTiles() {
      return bytes.getCount();
    }

    /**
     * Returns the biggest tiles in this aggregation by gzipped size (when aggregated over all layers) or raw size
     * within an individual layer.
     */
    public List<TileSummary> biggestTiles() {
      return topTiles.stream().sorted(Comparator.comparingLong(s -> -s.archivedSize)).toList();
    }

    /**
     * Returns average gzipped tile size in this aggregation, weighted by the {@link TileWeights} instance provided.
     * <p>
     * When multiple zoom-levels are combined, the weighted average respects the weight-per-zoom-level from
     * {@link TileWeights} so that low zoom tiles are not overweighted when analyzing a small extract.
     */
    public double weightedAverageArchivedSize() {
      return totalWeight == 0 ? archivedBytes.getAverage() : (weightedArchivedBytesSum * 1d / totalWeight);
    }

    /**
     * Returns average raw (not gzipped) tile size in this aggregation, weighted by the {@link TileWeights} instance
     * provided.
     *
     * @see #weightedAverageArchivedSize()
     */
    public double weightedAverageSize() {
      return totalWeight == 0 ? bytes.getAverage() : (weightedBytesSum * 1d / totalWeight);
    }

    private Cell mergeIn(Cell other) {
      return mergeIn(other, 1);
    }

    private Cell mergeIn(Cell other, double weight) {
      totalWeight += other.totalWeight * weight;
      weightedBytesSum += other.weightedBytesSum * weight;
      weightedArchivedBytesSum += other.weightedArchivedBytesSum * weight;
      archivedBytes.combine(other.archivedBytes);
      bytes.combine(other.bytes);
      for (var bigTile : other.topTiles) {
        acceptBigTile(bigTile.coord, bigTile.archivedSize, bigTile.layers);
      }
      return this;
    }

    private void acceptBigTile(TileCoord coord, int archivedBytes, List<TileSizeStats.LayerStats> layerStats) {
      if (archivedBytes >= bigTileCutoff) {
        topTiles.offer(new TileSummary(coord, archivedBytes, layerStats));
        while (topTiles.size() > TOP_N_TILES) {
          topTiles.poll();
          var min = topTiles.peek();
          if (min != null) {
            bigTileCutoff = min.archivedSize();
          }
        }
      }
    }

    String formatBiggestTiles(String debugUrlPattern) {
      var biggestTiles = biggestTiles();
      var formatter = Format.defaultInstance();
      return IntStream.range(0, biggestTiles.size())
        .mapToObj(index -> {
          var tile = biggestTiles.get(index);
          return "%d. %d/%d/%d (%s) %s (%s)".formatted(
            index + 1,
            tile.coord.z(),
            tile.coord.x(),
            tile.coord.y(),
            formatter.storage(tile.archivedSize),
            tile.coord.getDebugUrl(debugUrlPattern),
            tileBiggestLayers(formatter, tile)
          );
        }).collect(Collectors.joining("\n"));
    }
  }

  /** Statistics for a tile and its layers. */
  public record TileSummary(TileCoord coord, int archivedSize, List<TileSizeStats.LayerStats> layers)
    implements Comparable<TileSummary> {

    @Override
    public int compareTo(TileSummary o) {
      int result = Integer.compare(archivedSize, o.archivedSize);
      if (result == 0) {
        result = Integer.compare(coord.encoded(), o.coord.encoded());
      }
      return result;
    }

    TileSummary withSize(int newSize) {
      return new TileSummary(coord, newSize, layers);
    }
  }

  /** Overall summary statistics for a tileset, aggregated from all {@link Updater Updaters}. */
  public class Summary {

    private final List<Cell> byTile =
      IntStream.rangeClosed(PlanetilerConfig.MIN_MINZOOM, PlanetilerConfig.MAX_MAXZOOM)
        .mapToObj(i -> new Cell())
        .toList();

    private final List<Map<String, Cell>> byLayer =
      IntStream.rangeClosed(PlanetilerConfig.MIN_MINZOOM, PlanetilerConfig.MAX_MAXZOOM)
        .<Map<String, Cell>>mapToObj(i -> new HashMap<>())
        .toList();

    /** All the layers that appear in the tileset. */
    public List<String> layers() {
      return byLayer.stream().flatMap(e -> e.keySet().stream()).distinct().sorted().toList();
    }

    /** Returns the summary statistics for a layer at a zoom level. */
    public Cell get(int z, String layer) {
      return byLayer.get(z).getOrDefault(layer, new Cell());
    }

    /** Returns the summary statistics for a layer from all zoom levels. */
    public Cell get(String layer) {
      return combineZooms(byLayer.stream()
        .map(e -> e.getOrDefault(layer, new Cell()))
        .toList());
    }

    /** Returns the summary statistics for a zoom level from all layers. */
    public Cell get(int z) {
      return byTile.get(z);
    }

    /** Returns the summary statistics for the entire dataset by aggregating all layers and zoom-levels. */
    public Cell get() {
      return combineZooms(byTile);
    }

    /** Returns the minimum zoom a tile appears at in the tileset. */
    public int minZoomWithData() {
      return IntStream.range(0, byTile.size())
        .filter(i -> byTile.get(i).numTiles() > 0)
        .min()
        .orElse(PlanetilerConfig.MAX_MAXZOOM);
    }

    /** Returns the maximum zoom a tile appears at in the tileset. */
    public int maxZoomWithData() {
      return IntStream.range(0, byTile.size())
        .filter(i -> byTile.get(i).numTiles() > 0)
        .max()
        .orElse(PlanetilerConfig.MAX_MAXZOOM);
    }

    /** Returns the minimum zoom a specific layer appears at in the tileset. */
    public int minZoomWithData(String layer) {
      return IntStream.range(0, byLayer.size())
        .filter(i -> byLayer.get(i).containsKey(layer))
        .min()
        .orElse(PlanetilerConfig.MAX_MAXZOOM);
    }

    private Summary mergeIn(Summary other) {
      for (int z = PlanetilerConfig.MIN_MINZOOM; z <= PlanetilerConfig.MAX_MAXZOOM; z++) {
        byTile.get(z).mergeIn(other.byTile.get(z));
      }
      for (int z = PlanetilerConfig.MIN_MINZOOM; z <= PlanetilerConfig.MAX_MAXZOOM; z++) {
        var ourMap = byLayer.get(z);
        var theirMap = other.byLayer.get(z);
        theirMap.forEach((layer, stats) -> ourMap.merge(layer, stats, Cell::combine));
      }
      return this;
    }

    private Cell combineZooms(List<Cell> byTile) {
      // aggregate Cells over zoom levels, but respect the global zoom-level weights
      // from TileWeights
      double sumWeight = 0;
      double preSumWeight = 0;
      for (int z = 0; z < byTile.size(); z++) {
        var cell = byTile.get(z);
        long zoomWeight = tileWeights.getZoomWeight(z);
        if (cell.numTiles() > 0 && zoomWeight > 0) {
          sumWeight += zoomWeight;
          preSumWeight += cell.totalWeight;
        }
      }
      boolean noData = sumWeight == 0 || preSumWeight == 0;
      Cell result = new Cell();
      for (int z = 0; z < byTile.size(); z++) {
        var cell = byTile.get(z);
        long zoomWeight = tileWeights.getZoomWeight(z);
        if ((cell.numTiles() > 0 && zoomWeight > 0) || noData) {
          double weight = noData ? 1 : (zoomWeight / sumWeight) / (cell.totalWeight / preSumWeight);
          result.mergeIn(cell, weight);
        }
      }
      return result;
    }

    String formatRow(
      String firstColumn,
      Function<Number, String> formatter,
      Function<Integer, Number> extractCells,
      Number lastColumn
    ) {
      return formatRow(firstColumn, extractCells.andThen(formatter), formatter.apply(lastColumn));
    }

    String formatRow(
      String firstColumn,
      Function<Integer, String> extractStat,
      String lastColumn
    ) {
      StringBuilder builder = new StringBuilder();
      int minZoom = minZoomWithData();
      int maxZoom = maxZoomWithData();
      List<String> layers = layers().stream()
        .sorted(Comparator.comparingInt(this::minZoomWithData))
        .toList();
      int maxLayerLength = Math.max(9, layers.stream().mapToInt(String::length).max().orElse(0));
      String cellFormat = "%1$5s";
      String layerFormat = "%1$" + maxLayerLength + "s";

      builder.append(layerFormat.formatted(firstColumn));
      for (int z = minZoom; z <= maxZoom; z++) {
        builder.append(cellFormat.formatted(extractStat.apply(z)));
        builder.append(' ');
      }
      builder.append(cellFormat.formatted(lastColumn));
      return builder.toString();
    }

    String formatTable(Function<Number, String> formatter,
      Function<Cell, Number> extractStat) {
      StringBuilder builder = new StringBuilder();
      List<String> layers = layers().stream()
        .sorted(Comparator.comparingInt(this::minZoomWithData))
        .toList();

      // header:   0 1 2 3 4 ... 15
      builder.append(formatRow("", z -> "z" + z, "all")).append('\n');

      // each row: layer
      for (var layer : layers) {
        builder.append(formatRow(
          layer,
          formatter,
          z -> extractStat.apply(get(z, layer)),
          extractStat.apply(get(layer))
        )).append('\n');
      }
      return builder.toString().stripTrailing();
    }
  }

  /** Thread local updater that accepts individual statistics for each tile. */
  public class Updater {
    private final Summary summary = new Summary();

    private Updater() {
      summaries.add(summary);
    }

    private static Cell newCell(String layer) {
      return new Cell();
    }

    public void recordTile(TileCoord coord, int archivedBytes, List<TileSizeStats.LayerStats> layerStats) {
      var tileStat = summary.byTile.get(coord.z());
      var layerStat = summary.byLayer.get(coord.z());
      tileStat.archivedBytes.accept(archivedBytes);
      tileStat.acceptBigTile(coord, archivedBytes, layerStats);
      long weight = tileWeights.getWeight(coord);
      tileStat.totalWeight += weight;
      tileStat.weightedArchivedBytesSum += weight * archivedBytes;

      int sum = 0;
      for (var layer : layerStats) {
        var cell = layerStat.computeIfAbsent(layer.layer(), Updater::newCell);
        cell.bytes.accept(layer.layerBytes());
        cell.acceptBigTile(coord, layer.layerBytes(), layerStats);
        sum += layer.layerBytes();
        cell.weightedBytesSum += weight * layer.layerBytes();
        cell.totalWeight += weight;
      }
      tileStat.weightedBytesSum += weight * sum;
      tileStat.bytes.accept(sum);
    }
  }
}
