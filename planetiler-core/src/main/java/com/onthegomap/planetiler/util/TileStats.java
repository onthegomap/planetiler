package com.onthegomap.planetiler.util;

import static com.onthegomap.planetiler.worker.Worker.joinFutures;

import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
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
import java.util.HashMap;
import java.util.List;
import java.util.LongSummaryStatistics;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import vector_tile.VectorTileProto;

public class TileStats {

  private static final int BATCH_SIZE = 1_000;
  private static final int WARN_BYTES = 100_000;
  private static final int ERROR_BYTES = 500_000;

  private static final Logger LOGGER = LoggerFactory.getLogger(TileStats.class);

  private static final CsvMapper MAPPER = new CsvMapper();
  private static final CsvSchema SCHEMA = MAPPER
    .schemaFor(OutputRow.class)
    .withoutHeader()
    .withColumnSeparator('\t')
    .withLineSeparator("\n");
  public static final ObjectWriter WRITER = MAPPER.writer(SCHEMA);
  private static final int TOP_N_TILES = 10;
  private final List<Summary> summaries = new CopyOnWriteArrayList<>();

  public TileStats() {
    //    TODO load OSM tile weights
  }

  public static Path getOutputPath(Path output) {
    return output.resolveSibling(output.getFileName() + ".layerstats.tsv.gz");
  }

  public static void main(String... args) throws IOException {
    var tileStats = new TileStats();
    var arguments = Arguments.fromArgsOrConfigFile(args);
    var config = PlanetilerConfig.from(arguments);
    var stats = Stats.inMemory();
    var inputString = arguments.getString("input", "input file");
    var input = TileArchiveConfig.from(inputString);
    var localPath = input.getLocalPath();
    var output = localPath == null ?
      arguments.file("output", "output file") :
      arguments.file("output", "output file", getOutputPath(localPath));
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
            lines.addAll(TileStats.formatOutputRows(tile.coord(), zipped.length, layerStats));
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
    if (LOGGER.isDebugEnabled()) {
      tileStats.printStats(config.debugUrlPattern());
    }
    stats.printSummary();
  }

  public static List<String> formatOutputRows(TileCoord tileCoord, int archivedBytes, List<LayerStats> layerStats)
    throws IOException {
    int hilbert = tileCoord.hilbertEncoded();
    List<String> result = new ArrayList<>();
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
        layer.layerAttrBytes,
        layer.layerAttrKeys,
        layer.layerAttrValues
      )));
    }
    return result;
  }

  public static Writer newWriter(Path path) throws IOException {
    return new OutputStreamWriter(
      new FastGzipOutputStream(new BufferedOutputStream(Files.newOutputStream(path,
        StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.CREATE))));
  }

  public static String lineToString(OutputRow output) throws IOException {
    return WRITER.writeValueAsString(output);
  }

  public static String headerRow() {
    return String.join(
      String.valueOf(SCHEMA.getColumnSeparator()),
      SCHEMA.getColumnNames()
    ) + new String(SCHEMA.getLineSeparator());
  }

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
      result.add(new LayerStats(
        layer.getName(),
        layer.getSerializedSize(),
        layer.getFeaturesCount(),
        attrSize,
        layer.getKeysCount(),
        layer.getValuesCount()
      ));
    }
    result.sort(Comparator.naturalOrder());
    return result;
  }

  public void printStats(String debugUrlPattern) {
    if (LOGGER.isDebugEnabled()) {
      Summary result = summary();
      var overallStats = result.get();
      var formatter = Format.defaultInstance();
      var biggestTiles = overallStats.biggestTiles();
      LOGGER.debug("Biggest tiles (gzipped):\n{}",
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
        LOGGER.info("Other tiles with large layers:\n{}",
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

      LOGGER.debug("Max tile sizes:\n{}\n{}\n{}",
        writeStatsTable(result, n -> {
          String string = " " + formatter.storage(n, true);
          return n.intValue() > ERROR_BYTES ? AnsiColors.red(string) :
            n.intValue() > WARN_BYTES ? AnsiColors.yellow(string) :
            string;
        }, SummaryCell::maxSize),
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
    int minSize = tile.layers.stream().mapToInt(l -> l.layerBytes).max().orElse(0);
    return tile.layers.stream()
      .filter(d -> d.layerBytes >= minSize)
      .sorted(Comparator.comparingInt(d -> -d.layerBytes))
      .map(d -> d.layer + ":" + formatter.storage(d.layerBytes))
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
    Function<SummaryCell, Number> extractStat) {
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
    private final List<SummaryCell> byTile =
      IntStream.rangeClosed(PlanetilerConfig.MIN_MINZOOM, PlanetilerConfig.MAX_MAXZOOM)
        .mapToObj(i -> new SummaryCell())
        .toList();

    private final List<Map<String, SummaryCell>> byLayer =
      IntStream.rangeClosed(PlanetilerConfig.MIN_MINZOOM, PlanetilerConfig.MAX_MAXZOOM)
        .<Map<String, SummaryCell>>mapToObj(i -> new HashMap<>())
        .toList();

    public Summary merge(Summary other) {
      for (int z = PlanetilerConfig.MIN_MINZOOM; z <= PlanetilerConfig.MAX_MAXZOOM; z++) {
        byTile.get(z).merge(other.byTile.get(z));
      }
      for (int z = PlanetilerConfig.MIN_MINZOOM; z <= PlanetilerConfig.MAX_MAXZOOM; z++) {
        var ourMap = byLayer.get(z);
        var theirMap = other.byLayer.get(z);
        theirMap.forEach((layer, stats) -> ourMap.merge(layer, stats, SummaryCell::combine));
      }
      return this;
    }

    public static Summary combine(Summary a, Summary b) {
      return new Summary().merge(a).merge(b);
    }


    public List<String> layers() {
      return byLayer.stream().flatMap(e -> e.keySet().stream()).distinct().sorted().toList();
    }

    public SummaryCell get(int z, String layer) {
      return byLayer.get(z).getOrDefault(layer, new SummaryCell());
    }

    public SummaryCell get(String layer) {
      return byLayer.stream()
        .map(e -> e.getOrDefault(layer, new SummaryCell()))
        .reduce(new SummaryCell(), SummaryCell::combine);
    }

    public SummaryCell get(int z) {
      return byTile.get(z);
    }

    public SummaryCell get() {
      return byTile.stream().reduce(new SummaryCell(), SummaryCell::combine);
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
  }

  public static class SummaryCell {
    private final LongSummaryStatistics archivedBytes = new LongSummaryStatistics();
    private final LongSummaryStatistics bytes = new LongSummaryStatistics();
    private int bigTileCutoff = 0;
    private final PriorityQueue<TileSummary> topTiles = new PriorityQueue<>();

    SummaryCell(String layer) {}

    SummaryCell() {}

    public long maxSize() {
      return Math.max(0, bytes.getMax());
    }

    public long maxArchivedSize() {
      return Math.max(0, archivedBytes.getMax());
    }

    public long numTiles() {
      return bytes.getCount();
    }

    public SummaryCell merge(SummaryCell other) {
      archivedBytes.combine(other.archivedBytes);
      bytes.combine(other.bytes);
      for (var bigTile : other.topTiles) {
        acceptBigTile(bigTile.coord, bigTile.size, bigTile.layers);
      }
      return this;
    }

    private void acceptBigTile(TileCoord coord, int archivedBytes, List<LayerStats> layerStats) {
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

    public static SummaryCell combine(SummaryCell a, SummaryCell b) {
      return new SummaryCell().merge(a).merge(b);
    }

    public List<TileSummary> biggestTiles() {
      return topTiles.stream().sorted(Comparator.comparingLong(s -> -s.size)).toList();
    }
  }


  public Summary summary() {
    return summaries.stream().reduce(new Summary(), Summary::merge);
  }

  @JsonPropertyOrder({
    "z",
    "x",
    "y",
    "hilbert",
    "archived_tile_bytes",
    "layer",
    "layer_bytes",
    "layer_features",
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
    int layerAttrBytes,
    int layerAttrKeys,
    int layerAttrValues
  ) {}

  public record LayerStats(
    String layer,
    int layerBytes,
    int layerFeatures,
    int layerAttrBytes,
    int layerAttrKeys,
    int layerAttrValues
  ) implements Comparable<LayerStats> {

    @Override
    public int compareTo(LayerStats o) {
      return layer.compareTo(o.layer);
    }
  }

  record TileSummary(TileCoord coord, int size, List<LayerStats> layers) implements Comparable<TileSummary> {

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

    public void recordTile(TileCoord coord, int archivedBytes, List<LayerStats> layerStats) {
      var tileStat = summary.byTile.get(coord.z());
      var layerStat = summary.byLayer.get(coord.z());
      tileStat.archivedBytes.accept(archivedBytes);
      tileStat.acceptBigTile(coord, archivedBytes, layerStats);

      int sum = 0;
      for (var layer : layerStats) {
        var cell = layerStat.computeIfAbsent(layer.layer, SummaryCell::new);
        cell.bytes.accept(layer.layerBytes);
        cell.acceptBigTile(coord, layer.layerBytes, layerStats);
        sum += layer.layerBytes;
      }
      tileStat.bytes.accept(sum);
    }
  }
}
