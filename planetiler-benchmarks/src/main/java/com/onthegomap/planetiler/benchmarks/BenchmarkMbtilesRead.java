package com.onthegomap.planetiler.benchmarks;

import com.google.common.base.Stopwatch;
import com.onthegomap.planetiler.config.Arguments;
import com.onthegomap.planetiler.geo.TileCoord;
import com.onthegomap.planetiler.mbtiles.Mbtiles;
import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BenchmarkMbtilesRead {

  private static final Logger LOGGER = LoggerFactory.getLogger(BenchmarkMbtilesRead.class);

  private static final String SELECT_RANDOM_COORDS =
    "select tile_column, tile_row, zoom_level from tiles order by random() limit ?";

  public static void main(String[] args) throws Exception {

    Arguments arguments = Arguments.fromArgs(args);
    int repetitions = arguments.getInteger("bench_repetitions", "number of repetitions", 10);
    int nrTileReads = arguments.getInteger("bench_nr_tile_reads", "number of tiles to read", 500_000);
    int preWarms = arguments.getInteger("bench_pre_warms", "number of pre warm runs", 3);


    List<String> mbtilesPaths = new ArrayList<>();
    for (int i = 0;; i++) {
      String mbtilesPathStr = arguments.getString("bench_mbtiles" + i, "the mbtiles file to read from", null);
      if (mbtilesPathStr == null) {
        break;
      }
      mbtilesPaths.add(mbtilesPathStr);
    }

    if (mbtilesPaths.isEmpty()) {
      throw new IllegalArgumentException("pass one or many paths to the same mbtiles file");
    }

    mbtilesPaths.stream().map(File::new).forEach(f -> {
      if (!f.exists() || !f.isFile()) {
        throw new IllegalArgumentException("%s does not exists".formatted(f));
      }
    });

    List<TileCoord> randomCoordsToFetchPerRepetition = new LinkedList<>();

    try (var db = Mbtiles.newReadOnlyDatabase(Path.of(mbtilesPaths.get(0)))) {
      try (var statement = db.connection().prepareStatement(SELECT_RANDOM_COORDS)) {
        statement.setInt(1, nrTileReads);
        var rs = statement.executeQuery();
        while (rs.next()) {
          int x = rs.getInt("tile_column");
          int y = rs.getInt("tile_row");
          int z = rs.getInt("zoom_level");
          randomCoordsToFetchPerRepetition.add(TileCoord.ofXYZ(x, (1 << z) - 1 - y, z));
        }
      }
    }

    Map<String, Double> avgReadOperationsPerSecondPerDb = new HashMap<>();
    for (String dbPathStr : mbtilesPaths) {
      Path dbPath = Path.of(dbPathStr);
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

      avgReadOperationsPerSecondPerDb.put(dbPathStr, readOperationsPerSecondStats.getAverage());
    }

    List<String> keysSorted = avgReadOperationsPerSecondPerDb.entrySet().stream()
      .sorted((o1, o2) -> o1.getValue().compareTo(o2.getValue()))
      .map(Map.Entry::getKey)
      .toList();

    LOGGER.info("diffs");
    for (int i = 0; i < keysSorted.size() - 1; i++) {
      for (int j = i + 1; j < keysSorted.size(); j++) {
        String db0 = keysSorted.get(i);
        double avg0 = avgReadOperationsPerSecondPerDb.get(db0);
        String db1 = keysSorted.get(j);
        double avg1 = avgReadOperationsPerSecondPerDb.get(db1);

        double diff = avg1 * 100 / avg0 - 100;

        LOGGER.info("\"{}\" vs \"{}\": avg read operations per second improved by {}%", db0, db1, diff);
      }
    }
  }

  private static ReadResult readEachTile(List<TileCoord> coordsToFetch, Path dbPath) throws IOException {
    try (var db = Mbtiles.newReadOnlyDatabase(dbPath)) {
      db.getTile(0, 0, 0); // trigger prepared statement creation
      var totalSw = Stopwatch.createStarted();
      for (var coordToFetch : coordsToFetch) {
        if (db.getTile(coordToFetch) == null) {
          throw new IllegalStateException("%s should exist in %s".formatted(coordToFetch, dbPath));
        }
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
