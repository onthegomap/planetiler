package com.onthegomap.planetiler.benchmarks;

import com.google.common.base.Stopwatch;
import com.onthegomap.planetiler.config.Arguments;
import com.onthegomap.planetiler.geo.TileCoord;
import com.onthegomap.planetiler.mbtiles.Mbtiles;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BenchmarkMbtilesRead {

  private static final Logger LOGGER = LoggerFactory.getLogger(BenchmarkMbtilesRead.class);

  private static final String SELECT_RANDOM_COORDS =
    "select tile_column, tile_row, zoom_level from tiles order by random() limit ?";

  public static void main(String[] args) throws Exception {

    Arguments arguments = Arguments.fromArgs(args);
    int repetitions = arguments.getInteger("bench_repetitions", "number of repetitions", 10);
    Path tilesPath = arguments.file("bench_tiles_path", "path to z,x,y tiles");
    int nrTileReads = arguments.getInteger("bench_nr_tile_reads", "number of tiles to read",
      tilesPath != null ? lineCount(tilesPath) : 500_000);
    int preWarms = arguments.getInteger("bench_pre_warms", "number of pre warm runs", 3);

    List<Path> mbtilesPaths = arguments.getList("bench_mbtiles", "the mbtiles file to read from", List.of()).stream()
      .map(Paths::get).toList();

    if (mbtilesPaths.isEmpty()) {
      throw new IllegalArgumentException("pass one or many paths to the same mbtiles file");
    }

    for (var p : mbtilesPaths) {
      if (!Files.exists(p) || !Files.isRegularFile(p)) {
        throw new IllegalArgumentException("%s does not exists".formatted(p));
      }
    }

    List<TileCoord> randomCoordsToFetchPerRepetition = new LinkedList<>();

    do {
      int toFetch = nrTileReads - randomCoordsToFetchPerRepetition.size();
      if (tilesPath != null) {
        Predicate<String> isValidLine = Pattern.compile("^\\d+,\\d+,\\d+$").asMatchPredicate();
        try (var lines = Files.lines(tilesPath)) {
          lines
            .filter(isValidLine)
            .map(s -> s.split(","))
            .limit(toFetch)
            .forEach(parts -> {
              randomCoordsToFetchPerRepetition
                .add(TileCoord.ofXYZ(Integer.parseInt(parts[2]), Integer.parseInt(parts[0]),
                  Integer.parseInt(parts[1])));
            });
        }
      } else {
        try (var db = Mbtiles.newReadOnlyDatabase(mbtilesPaths.get(0))) {
          try (var statement = db.connection().prepareStatement(SELECT_RANDOM_COORDS)) {
            statement.setInt(1, toFetch);
            var rs = statement.executeQuery();
            while (rs.next()) {
              int x = rs.getInt("tile_column");
              int y = rs.getInt("tile_row");
              int z = rs.getInt("zoom_level");
              randomCoordsToFetchPerRepetition.add(TileCoord.ofXYZ(x, (1 << z) - 1 - y, z));
            }
          }
        }
      }
    } while (randomCoordsToFetchPerRepetition.size() < nrTileReads);

    Map<Path, Double> avgReadOperationsPerSecondPerDb = new HashMap<>();
    for (Path dbPath : mbtilesPaths) {
      List<ReadResult> results = new LinkedList<>();

      LOGGER.info("working on {}", dbPath);

      for (int preWarm = 0; preWarm < preWarms; preWarm++) {
        readEachTile(randomCoordsToFetchPerRepetition, dbPath);
      }

      for (int rep = 0; rep < repetitions; rep++) {
        results.add(readEachTile(randomCoordsToFetchPerRepetition, dbPath));
      }
      var readOperationsPerSecondStats =
        results.stream().mapToDouble(ReadResult::readOperationsPerSecond).summaryStatistics();
      LOGGER.info("readOperationsPerSecondStats: {}", readOperationsPerSecondStats);

      avgReadOperationsPerSecondPerDb.put(dbPath, readOperationsPerSecondStats.getAverage());
    }

    List<Path> keysSorted = avgReadOperationsPerSecondPerDb.entrySet().stream()
      .sorted((o1, o2) -> o1.getValue().compareTo(o2.getValue()))
      .map(Map.Entry::getKey)
      .toList();

    LOGGER.info("diffs");
    for (int i = 0; i < keysSorted.size() - 1; i++) {
      for (int j = i + 1; j < keysSorted.size(); j++) {
        Path db0 = keysSorted.get(i);
        double avg0 = avgReadOperationsPerSecondPerDb.get(db0);
        Path db1 = keysSorted.get(j);
        double avg1 = avgReadOperationsPerSecondPerDb.get(db1);

        double diff = avg1 * 100 / avg0 - 100;

        LOGGER.info("\"{}\" to \"{}\": avg read operations per second improved by {}%", db0, db1, diff);
      }
    }
  }

  private static boolean isInt(String s) {
    try {
      Integer.parseInt(s);
      return true;
    } catch (NumberFormatException e) {
      return false;
    }
  }

  private static int lineCount(Path tilesPath) {
    try (var lines = Files.lines(tilesPath)) {
      return (int) lines.count();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private static ReadResult readEachTile(List<TileCoord> coordsToFetch, Path dbPath) throws IOException {
    try (var db = Mbtiles.newReadOnlyDatabase(dbPath)) {
      db.getTile(0, 0, 0); // trigger prepared statement creation
      var totalSw = Stopwatch.createStarted();
      for (var coordToFetch : coordsToFetch) {
        db.getTile(coordToFetch);
      }
      totalSw.stop();
      return new ReadResult(totalSw.elapsed(), coordsToFetch.size());
    }
  }

  private record ReadResult(Duration duration, int coordsFetchedCount) {
    double readOperationsPerSecond() {
      double secondsFractional = duration.toNanos() / 1E9;
      return coordsFetchedCount / secondsFractional;
    }
  }
}
