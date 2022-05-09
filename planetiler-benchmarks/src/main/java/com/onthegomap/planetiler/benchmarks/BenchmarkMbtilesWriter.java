package com.onthegomap.planetiler.benchmarks;

import com.onthegomap.planetiler.Profile;
import com.onthegomap.planetiler.VectorTile;
import com.onthegomap.planetiler.collection.FeatureGroup;
import com.onthegomap.planetiler.config.Arguments;
import com.onthegomap.planetiler.config.MbtilesMetadata;
import com.onthegomap.planetiler.config.PlanetilerConfig;
import com.onthegomap.planetiler.geo.GeometryType;
import com.onthegomap.planetiler.geo.TileCoord;
import com.onthegomap.planetiler.mbtiles.MbtilesWriter;
import com.onthegomap.planetiler.render.RenderedFeature;
import com.onthegomap.planetiler.stats.Counter;
import com.onthegomap.planetiler.stats.Stats;
import com.onthegomap.planetiler.stats.Timers;
import com.onthegomap.planetiler.util.MemoryEstimator.HasEstimate;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.LongSummaryStatistics;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BenchmarkMbtilesWriter {

  private static final Logger LOGGER = LoggerFactory.getLogger(BenchmarkMbtilesWriter.class);


  public static void main(String[] args) throws IOException {

    Arguments arguments = Arguments.fromArgs(args);

    int tilesToWrite = arguments.getInteger("bench_tiles_to_write", "number of tiles to write", 1_000_000);
    int repetitions = arguments.getInteger("bench_repetitions", "number of repetitions", 10);
    // to put some context here: Australia has 8% distinct tiles
    int distinctTilesInPercent = arguments.getInteger("bench_distinct_tiles", "distinct tiles in percent", 10);


    MbtilesMetadata mbtilesMetadata = new MbtilesMetadata(new Profile.NullProfile());
    PlanetilerConfig config = PlanetilerConfig.from(arguments);

    FeatureGroup featureGroup = FeatureGroup.newInMemoryFeatureGroup(new Profile.NullProfile(), Stats.inMemory());
    renderTiles(featureGroup, tilesToWrite, distinctTilesInPercent, config.minzoom(), config.maxzoom());

    RepeatedMbtilesWriteStats repeatedMbtilesStats = new RepeatedMbtilesWriteStats();
    for (int repetition = 0; repetition < repetitions; repetition++) {
      MyStats myStats = new MyStats();
      Path outputPath = getTempOutputPath();
      MbtilesWriter.writeOutput(featureGroup, outputPath, mbtilesMetadata, config, myStats);
      repeatedMbtilesStats.updateWithStats(myStats, outputPath);
      outputPath.toFile().delete();
    }

    LOGGER.info("{}", repeatedMbtilesStats);
  }


  private static void renderTiles(FeatureGroup featureGroup, int tilesToWrite, int distinctTilesInPercent, int minzoom,
    int maxzoom) throws IOException {

    String lastDistinctAttributeValue = "0";
    String prevLastDistinctAttributeValue = "0";

    try (
      var renderer = featureGroup.newRenderedFeatureEncoder();
      var writer = featureGroup.writerForThread();
    ) {
      int tilesWritten = 0;
      for (int z = minzoom; z <= maxzoom; z++) {
        int maxCoord = 1 << z;
        for (int x = 0; x < maxCoord; x++) {
          for (int y = 0; y < maxCoord; y++) {

            String attributeValue;
            if (tilesWritten % 100 < distinctTilesInPercent) {
              attributeValue = Integer.toString(tilesWritten);
              prevLastDistinctAttributeValue = lastDistinctAttributeValue;
              lastDistinctAttributeValue = attributeValue;
            } else if (tilesWritten % 2 == 0) { // make sure the existing de-duping mechanism won't work
              attributeValue = prevLastDistinctAttributeValue;
            } else {
              attributeValue = lastDistinctAttributeValue;
            }

            var renderedFeatures = createRenderedFeature(x, y, z, attributeValue);
            var sortableFeature = renderer.apply(renderedFeatures);
            writer.accept(sortableFeature);
            if (++tilesWritten >= tilesToWrite) {
              return;
            }
          }
        }
      }
    }
  }

  private static RenderedFeature createRenderedFeature(int x, int y, int z, String attributeValue) {
    var geometry = new VectorTile.VectorGeometry(new int[0], GeometryType.POINT, 14);
    var vectorTileFeature = new VectorTile.Feature("layer", 0, geometry, Map.of("k", attributeValue));
    return new RenderedFeature(TileCoord.ofXYZ(x, y, z), vectorTileFeature, 0, Optional.empty());
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

  private record RepeatedMbtilesWriteStats(
    LongSummaryStatistics total,
    LongSummaryStatistics read,
    LongSummaryStatistics encode,
    LongSummaryStatistics write,
    LongSummaryStatistics memoizedTiles,
    LongSummaryStatistics file
  ) {
    RepeatedMbtilesWriteStats() {
      this(
        new LongSummaryStatistics(),
        new LongSummaryStatistics(),
        new LongSummaryStatistics(),
        new LongSummaryStatistics(),
        new LongSummaryStatistics(),
        new LongSummaryStatistics()
      );
    }

    void updateWithStats(MyStats myStats, Path mbtilesPath) {
      total.accept(myStats.getStageDuration("mbtiles").toMillis());
      memoizedTiles.accept(myStats.getLongCounter("mbtiles_memoized_tiles"));
      MyTimers myTimers = myStats.timers();
      read.accept(myTimers.getWorkerDuration("mbtiles_read").toMillis());
      encode.accept(myTimers.getWorkerDuration("mbtiles_encode").toMillis());
      write.accept(myTimers.getWorkerDuration("mbtiles_write").toMillis());
      try {
        file.accept(Files.size(mbtilesPath));
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  private static class MyTimers extends Timers {
    private final Map<String, Duration> workerDurations = new ConcurrentHashMap<>();

    @Override
    public void finishedWorker(String prefix, Duration elapsed) {
      workerDurations.put(prefix, elapsed);
      super.finishedWorker(prefix, elapsed);
    }

    Duration getWorkerDuration(String prefix) {
      return workerDurations.get(prefix);
    }
  }
  /*
   * custom stats in order to have custom times in order to get worker durations
   * and while at it, make stage durations available as well
   * note: the actual problem here is that Timer.Stage/ThreadInfo are not public
   */
  private static class MyStats implements Stats {

    private final Map<String, Duration> stageDurations = new ConcurrentHashMap<>();
    private final Map<String, Counter.MultiThreadCounter> longCounters = new ConcurrentHashMap<>();

    private final MyTimers timers = new MyTimers();

    Duration getStageDuration(String name) {
      return stageDurations.get(name);
    }

    long getLongCounter(String name) {
      var counter = longCounters.get(name);
      if (counter == null) {
        return -1;
      }
      return counter.get();
    }

    @Override
    public Timers.Finishable startStage(String name) {
      Instant start = Instant.now();
      Timers.Finishable wrapped = Stats.super.startStage(name);
      return () -> {
        stageDurations.put(name, Duration.between(start, Instant.now()));
        wrapped.stop();
      };
    }

    @Override
    public void close() throws Exception {}

    @Override
    public void emittedFeatures(int z, String layer, int numFeatures) {}

    @Override
    public void processedElement(String elemType, String layer) {}

    @Override
    public void wroteTile(int zoom, int bytes) {}

    @Override
    public MyTimers timers() {
      return timers;
    }

    @Override
    public Map<String, Path> monitoredFiles() {
      return Map.of();
    }

    @Override
    public void monitorInMemoryObject(String name, HasEstimate object) {}

    @Override
    public void gauge(String name, Supplier<Number> value) {}

    @Override
    public void counter(String name, Supplier<Number> supplier) {}

    @Override
    public void counter(String name, String label, Supplier<Map<String, LongSupplier>> values) {}

    @Override
    public void dataError(String errorCode) {}

    @Override
    public Counter.MultiThreadCounter longCounter(String name) {
      var counter = Counter.newMultiThreadCounter();
      longCounters.put(name, counter);
      return counter;
    }

    @Override
    public Counter.MultiThreadCounter nanoCounter(String name) {
      return Counter.newMultiThreadCounter();
    }

  }

}
