package com.onthegomap.planetiler.benchmarks;

import com.google.common.base.Stopwatch;
import com.onthegomap.planetiler.config.Arguments;
import com.onthegomap.planetiler.geo.TileCoord;
import com.onthegomap.planetiler.mbtiles.Mbtiles;
import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.LongSummaryStatistics;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BenchmarkMbtilesRead {

  private static final Logger LOGGER = LoggerFactory.getLogger(BenchmarkMbtilesWriter.class);

  private static final String SELECT_RANDOM_COORDS =
    "select tile_column, tile_row, zoom_level from tiles order by random() limit ?";

  public static void main(String[] args) throws Exception {

    Arguments arguments = Arguments.fromArgs(args);
    int repetitions = arguments.getInteger("bench_repetitions", "number of repetitions", 100);
    int nrTileReads = arguments.getInteger("bench_nr_tile_reads", "number of tiles to read", 10000);


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

    Map<String, Double> avgIndividualReadPerDb = new HashMap<>();
    for (String dbPathStr : mbtilesPaths) {
      Path dbPath = Path.of(dbPathStr);
      List<ReadResult> results = new LinkedList<>();

      LOGGER.info("working on {}", dbPath);

      for (int rep = 0; rep < repetitions; rep++) {
        results.add(readEachTile(randomCoordsToFetchPerRepetition, dbPath));
      }
      var totalStats = results.stream().mapToLong(ReadResult::totalDuration).summaryStatistics();
      LOGGER.info("totalReadStats: {}", totalStats);

      LongSummaryStatistics individualStats = results.stream().map(ReadResult::individualReadStats)
        .collect(Collector.of(LongSummaryStatistics::new, LongSummaryStatistics::combine, (left, right) -> {
          left.combine(right);
          return left;
        }));
      LOGGER.info("individualReadStats:  {}", individualStats);

      avgIndividualReadPerDb.put(dbPathStr, individualStats.getAverage());
    }

    List<String> keysSorted = avgIndividualReadPerDb.entrySet().stream()
      .sorted((o1, o2) -> o1.getValue().compareTo(o2.getValue()))
      .map(Map.Entry::getKey)
      .toList();

    LOGGER.info("diffs");
    for (int i = 0; i < keysSorted.size() - 1; i++) {
      for (int j = i + 1; j < keysSorted.size(); j++) {
        String db0 = keysSorted.get(i);
        double avg0 = avgIndividualReadPerDb.get(db0);
        String db1 = keysSorted.get(j);
        double avg1 = avgIndividualReadPerDb.get(db1);

        double diff = avg1 * 100 / avg0 - 100;

        LOGGER.info("\"{}\" vs \"{}\": avgs reads up by {}%", db0, db1, diff);
      }
    }
  }

  private static ReadResult readEachTile(List<TileCoord> coordsToFetch, Path dbPath) throws IOException {
    LongSummaryStatistics individualFetchDurations = new LongSummaryStatistics();
    try (var db = Mbtiles.newReadOnlyDatabase(dbPath)) {
      db.getTile(0, 0, 0); // trigger prepared statement creation
      var totalSw = Stopwatch.createStarted();
      for (var coordToFetch : coordsToFetch) {
        var sw = Stopwatch.createStarted();
        if (db.getTile(coordToFetch) == null) {
          throw new IllegalStateException("%s should exist in %s".formatted(coordToFetch, dbPath));
        }
        sw.stop();
        individualFetchDurations.accept(sw.elapsed(TimeUnit.NANOSECONDS));
      }
      totalSw.stop();
      return new ReadResult(totalSw.elapsed(TimeUnit.NANOSECONDS), individualFetchDurations);
    }
  }

  private record ReadResult(long totalDuration, LongSummaryStatistics individualReadStats) {}


}
