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

public class TilesetSummaryStatistics {
  private static final int TOP_N_TILES = 10;
  private static final int WARN_BYTES = 100_000;
  private static final int ERROR_BYTES = 500_000;
  private static final Logger LOGGER = LoggerFactory.getLogger(TilesetSummaryStatistics.class);
  private final List<Summary> summaries = new CopyOnWriteArrayList<>();

  public Summary summary() {
    return summaries.stream().reduce(new Summary(), Summary::combine);
  }

  public void printStats(String debugUrlPattern) {
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("Tile stats:");
      Summary result = summary();
      var overallStats = result.get();
      var formatter = Format.defaultInstance();
      var biggestTiles = overallStats.biggestTiles();
      LOGGER.debug("Biggest tiles (gzipped)\n{}",
        IntStream.range(0, biggestTiles.size())
          .mapToObj(index -> {
            var tile = biggestTiles.get(index);
            return "%d. %d/%d/%d (%s) %s (%s)".formatted(
              index + 1,
              tile.coord.z(),
              tile.coord.x(),
              tile.coord.y(),
              formatter.storage(tile.size),
              tile.coord.getDebugUrl(debugUrlPattern),
              tileBiggestLayers(formatter, tile)
            );
          }).collect(Collectors.joining("\n"))
      );
      var alreadyListed = biggestTiles.stream().map(TileSummary::coord).collect(Collectors.toSet());
      var otherTiles = result.layers().stream()
        .flatMap(layer -> result.get(layer).biggestTiles().stream().limit(1))
        .filter(tile -> !alreadyListed.contains(tile.coord) && tile.size > WARN_BYTES)
        .toList();
      if (!otherTiles.isEmpty()) {
        LOGGER.info("Other tiles with large layers\n{}",
          otherTiles.stream()
            .map(tile -> "%d/%d/%d (%s) %s (%s)".formatted(
              tile.coord.z(),
              tile.coord.x(),
              tile.coord.y(),
              formatter.storage(tile.size),
              tile.coord.getDebugUrl(debugUrlPattern),
              tileBiggestLayers(formatter, tile)
            )).collect(Collectors.joining("\n")));
      }

      LOGGER.debug("Max tile sizes\n{}\n{}\n{}",
        writeStatsTable(result, n -> {
          String string = " " + formatter.storage(n, true);
          return n.intValue() > ERROR_BYTES ? AnsiColors.red(string) :
            n.intValue() > WARN_BYTES ? AnsiColors.yellow(string) :
            string;
        }, Cell::maxSize),
        writeStatsRow(result, "full tile",
          formatter::storage,
          z -> result.get(z).maxSize(),
          result.get().maxSize()
        ),
        writeStatsRow(result, "gzipped",
          formatter::storage,
          z -> result.get(z).maxArchivedSize(),
          result.get().maxArchivedSize()
        )
      );
      LOGGER.debug("    # tiles: {}", formatter.integer(overallStats.numTiles()));
      LOGGER.debug("   Max tile: {} (gzipped: {})",
        formatter.storage(overallStats.maxSize()),
        formatter.storage(overallStats.maxArchivedSize()));
      // TODO weighted average tile size
    }
  }

  private static String tileBiggestLayers(Format formatter, TileSummary tile) {
    int minSize = tile.layers.stream().mapToInt(l -> l.layerBytes()).max().orElse(0);
    return tile.layers.stream()
      .filter(d -> d.layerBytes() >= minSize)
      .sorted(Comparator.comparingInt(d -> -d.layerBytes()))
      .map(d -> d.layer() + ":" + formatter.storage(d.layerBytes()))
      .collect(Collectors.joining(", "));
  }

  private static String writeStatsRow(
    Summary result,
    String firstColumn,
    Function<Number, String> formatter,
    Function<Integer, Number> extractCells,
    Number lastColumn
  ) {
    return writeStatsRow(result, firstColumn, extractCells.andThen(formatter), formatter.apply(lastColumn));
  }

  private static String writeStatsRow(
    Summary result,
    String firstColumn,
    Function<Integer, String> extractStat,
    String lastColumn
  ) {
    StringBuilder builder = new StringBuilder();
    int minZoom = result.minZoomWithData();
    int maxZoom = result.maxZoomWithData();
    List<String> layers = result.layers().stream()
      .sorted(Comparator.comparingInt(result::minZoomWithData))
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

  private static String writeStatsTable(Summary result, Function<Number, String> formatter,
    Function<Cell, Number> extractStat) {
    StringBuilder builder = new StringBuilder();
    List<String> layers = result.layers().stream()
      .sorted(Comparator.comparingInt(result::minZoomWithData))
      .toList();

    // header:   0 1 2 3 4 ... 15
    builder.append(writeStatsRow(result, "", z -> "z" + z, "all")).append('\n');

    // each row: layer
    for (var layer : layers) {
      builder.append(writeStatsRow(
        result,
        layer,
        formatter,
        z -> extractStat.apply(result.get(z, layer)),
        extractStat.apply(result.get(layer))
      )).append('\n');
    }
    return builder.toString().stripTrailing();
  }

  public Updater threadLocalUpdater() {
    return new Updater();
  }

  public static class Summary {
    private final List<Cell> byTile =
      IntStream.rangeClosed(PlanetilerConfig.MIN_MINZOOM, PlanetilerConfig.MAX_MAXZOOM)
        .mapToObj(i -> new Cell())
        .toList();

    private final List<Map<String, Cell>> byLayer =
      IntStream.rangeClosed(PlanetilerConfig.MIN_MINZOOM, PlanetilerConfig.MAX_MAXZOOM)
        .<Map<String, Cell>>mapToObj(i -> new HashMap<>())
        .toList();

    public Summary mergeIn(Summary other) {
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

    public List<String> layers() {
      return byLayer.stream().flatMap(e -> e.keySet().stream()).distinct().sorted().toList();
    }

    public Cell get(int z, String layer) {
      return byLayer.get(z).getOrDefault(layer, new Cell());
    }

    public Cell get(String layer) {
      return byLayer.stream()
        .map(e -> e.getOrDefault(layer, new Cell()))
        .reduce(new Cell(), Cell::combine);
    }

    public Cell get(int z) {
      return byTile.get(z);
    }

    public Cell get() {
      return byTile.stream().reduce(new Cell(), Cell::combine);
    }

    public int minZoomWithData() {
      return IntStream.range(0, byTile.size())
        .filter(i -> byTile.get(i).numTiles() > 0)
        .min()
        .orElse(PlanetilerConfig.MAX_MAXZOOM);
    }

    public int maxZoomWithData() {
      return IntStream.range(0, byTile.size())
        .filter(i -> byTile.get(i).numTiles() > 0)
        .max()
        .orElse(PlanetilerConfig.MAX_MAXZOOM);
    }

    public int minZoomWithData(String layer) {
      return IntStream.range(0, byLayer.size())
        .filter(i -> byLayer.get(i).containsKey(layer))
        .min()
        .orElse(PlanetilerConfig.MAX_MAXZOOM);
    }

    private static Summary combine(Summary a, Summary b) {
      return new Summary().mergeIn(a).mergeIn(b);
    }
  }

  public static class Cell {
    private final LongSummaryStatistics archivedBytes = new LongSummaryStatistics();
    private final LongSummaryStatistics bytes = new LongSummaryStatistics();
    private final PriorityQueue<TileSummary> topTiles = new PriorityQueue<>();
    private int bigTileCutoff = 0;

    private static Cell combine(Cell a, Cell b) {
      return new Cell().mergeIn(a).mergeIn(b);
    }

    public long maxSize() {
      return Math.max(0, bytes.getMax());
    }

    public long maxArchivedSize() {
      return Math.max(0, archivedBytes.getMax());
    }

    public long numTiles() {
      return bytes.getCount();
    }

    private Cell mergeIn(Cell other) {
      archivedBytes.combine(other.archivedBytes);
      bytes.combine(other.bytes);
      for (var bigTile : other.topTiles) {
        acceptBigTile(bigTile.coord, bigTile.size, bigTile.layers);
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
            bigTileCutoff = min.size();
          }
        }
      }
    }

    public List<TileSummary> biggestTiles() {
      return topTiles.stream().sorted(Comparator.comparingLong(s -> -s.size)).toList();
    }
  }

  public record TileSummary(TileCoord coord, int size, List<TileSizeStats.LayerStats> layers)
    implements Comparable<TileSummary> {

    @Override
    public int compareTo(TileSummary o) {
      int result = Integer.compare(size, o.size);
      if (result == 0) {
        result = Integer.compare(coord.encoded(), o.coord.encoded());
      }
      return result;
    }

    TileSummary withSize(int newSize) {
      return new TileSummary(coord, newSize, layers);
    }
  }

  public class Updater {
    private final Summary summary = new Summary();

    private Updater() {
      summaries.add(summary);
    }

    public void recordTile(TileCoord coord, int archivedBytes, List<TileSizeStats.LayerStats> layerStats) {
      var tileStat = summary.byTile.get(coord.z());
      var layerStat = summary.byLayer.get(coord.z());
      tileStat.archivedBytes.accept(archivedBytes);
      tileStat.acceptBigTile(coord, archivedBytes, layerStats);

      int sum = 0;
      for (var layer : layerStats) {
        var cell = layerStat.computeIfAbsent(layer.layer(), Updater::newCell);
        cell.bytes.accept(layer.layerBytes());
        cell.acceptBigTile(coord, layer.layerBytes(), layerStats);
        sum += layer.layerBytes();
      }
      tileStat.bytes.accept(sum);
    }

    private static Cell newCell(String layer) {
      return new Cell();
    }
  }
}
