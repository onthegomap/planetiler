package com.onthegomap.planetiler.benchmarks;

import com.google.common.base.Stopwatch;
import com.onthegomap.planetiler.config.Arguments;
import com.onthegomap.planetiler.config.PlanetilerConfig;
import com.onthegomap.planetiler.geo.TileCoord;
import com.onthegomap.planetiler.mbtiles.Mbtiles;
import com.onthegomap.planetiler.mbtiles.Mbtiles.BatchedTileWriter;
import com.onthegomap.planetiler.mbtiles.TileEncodingResult;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.DoubleSummaryStatistics;
import java.util.OptionalLong;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BenchmarkMbtilesWriter {

  private static final Logger LOGGER = LoggerFactory.getLogger(BenchmarkMbtilesWriter.class);

  public static void main(String[] args) throws IOException {

    Arguments arguments = Arguments.fromArgs(args);

    int tilesToWrite = arguments.getInteger("bench_tiles_to_write", "number of tiles to write", 1_000_000);
    int repetitions = arguments.getInteger("bench_repetitions", "number of repetitions", 10);
    /*
     * select count(distinct(tile_data_id)) * 100.0 / count(*) from tiles_shallow
     * => ~8% (Australia)
     */
    int distinctTilesInPercent = arguments.getInteger("bench_distinct_tiles", "distinct tiles in percent", 10);
    /*
     * select avg(length(tile_data))
     * from (select tile_data_id from tiles_shallow group by tile_data_id having count(*) = 1) as x
     * join tiles_data using(tile_data_id)
     * => ~785 (Australia)
     */
    int distinctTileDataSize =
      arguments.getInteger("bench_distinct_tile_data_size", "distinct tile data size in bytes", 800);
    /*
     * select avg(length(tile_data))
     * from (select tile_data_id from tiles_shallow group by tile_data_id having count(*) > 1) as x
     * join tiles_shallow using(tile_data_id)
     * join tiles_data using(tile_data_id)
     * => ~93 (Australia)
     */
    int dupeTileDataSize = arguments.getInteger("bench_dupe_tile_data_size", "dupe tile data size in bytes", 100);
    /*
     * select count(*) * 100.0 / sum(usage_count)
     * from (select tile_data_id, count(*) as usage_count from tiles_shallow group by tile_data_id having count(*) > 1)
     * => ~0.17% (Australia)
     */
    int dupeSpreadInPercent = arguments.getInteger("bench_dupe_spread", "dupe spread in percent", 10);

    byte[] distinctTileData = createFilledByteArray(distinctTileDataSize);
    byte[] dupeTileData = createFilledByteArray(dupeTileDataSize);

    PlanetilerConfig config = PlanetilerConfig.from(arguments);

    DoubleSummaryStatistics tileWritesPerSecondsStats = new DoubleSummaryStatistics();

    for (int repetition = 0; repetition < repetitions; repetition++) {

      Path outputPath = getTempOutputPath();
      try (var mbtiles = Mbtiles.newWriteToFileDatabase(outputPath, config.compactDb())) {

        if (config.skipIndexCreation()) {
          mbtiles.createTablesWithoutIndexes();
        } else {
          mbtiles.createTablesWithIndexes();
        }

        try (var writer = mbtiles.newBatchedTileWriter()) {
          Stopwatch sw = Stopwatch.createStarted();
          writeTiles(writer, tilesToWrite, distinctTilesInPercent, distinctTileData, dupeTileData, dupeSpreadInPercent);
          sw.stop();
          double secondsFractional = sw.elapsed(TimeUnit.NANOSECONDS) / 1E9;
          double tileWritesPerSecond = tilesToWrite / secondsFractional;
          tileWritesPerSecondsStats.accept(tileWritesPerSecond);
        }

      } finally {
        Files.delete(outputPath);
      }
    }

    LOGGER.info("tileWritesPerSecondsStats: {}", tileWritesPerSecondsStats);
  }


  private static void writeTiles(BatchedTileWriter writer, int tilesToWrite, int distinctTilesInPercent,
    byte[] distinctTileData, byte[] dupeTileData, int dupeSpreadInPercent) {

    int dupesToWrite = (int) Math.round(tilesToWrite * (100 - distinctTilesInPercent) / 100.0);
    int dupeHashMod = (int) Math.round(dupesToWrite * dupeSpreadInPercent / 100.0);
    int tilesWritten = 0;
    int dupeCounter = 0;
    for (int z = 0; z <= 14; z++) {
      int maxCoord = 1 << z;
      for (int x = 0; x < maxCoord; x++) {
        for (int y = maxCoord - 1; y >= 0; y--) {

          TileCoord coord = TileCoord.ofXYZ(x, y, z);
          TileEncodingResult toWrite;
          if (tilesWritten % 100 < distinctTilesInPercent) {
            toWrite = new TileEncodingResult(coord, distinctTileData, OptionalLong.empty());
          } else {
            ++dupeCounter;
            int hash = dupeHashMod == 0 ? 0 : dupeCounter % dupeHashMod;
            toWrite = new TileEncodingResult(coord, dupeTileData, OptionalLong.of(hash));
          }

          writer.write(toWrite);

          if (++tilesWritten >= tilesToWrite) {
            return;
          }
        }
      }
    }
  }

  private static Path getTempOutputPath() {
    File f;
    try {
      f = File.createTempFile("planetiler", ".mbtiles");
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
    f.deleteOnExit();
    return f.toPath();
  }

  private static byte[] createFilledByteArray(int len) {
    byte[] data = new byte[len];
    new Random(0).nextBytes(data);
    return data;
  }
}
