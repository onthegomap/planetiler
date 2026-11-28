package com.onthegomap.planetiler.reader.parquet;

import static com.onthegomap.planetiler.TestUtils.newPoint;
import static com.onthegomap.planetiler.TestUtils.round;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.onthegomap.planetiler.FeatureCollector;
import com.onthegomap.planetiler.Profile;
import com.onthegomap.planetiler.TestUtils;
import com.onthegomap.planetiler.collection.FeatureGroup;
import com.onthegomap.planetiler.config.Arguments;
import com.onthegomap.planetiler.config.PlanetilerConfig;
import com.onthegomap.planetiler.geo.GeoUtils;
import com.onthegomap.planetiler.geo.GeometryException;
import com.onthegomap.planetiler.geo.TileOrder;
import com.onthegomap.planetiler.reader.SourceFeature;
import com.onthegomap.planetiler.reader.SourceFeatureProcessor;
import com.onthegomap.planetiler.reader.remote.DownloadMode;
import com.onthegomap.planetiler.reader.remote.RangeFile;
import com.onthegomap.planetiler.stats.ProgressLoggers;
import com.onthegomap.planetiler.stats.Stats;
import com.onthegomap.planetiler.util.CompletableFutures;
import com.onthegomap.planetiler.util.Glob;
import com.onthegomap.planetiler.util.Parse;
import com.onthegomap.planetiler.util.StubDownloader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.locationtech.jts.geom.Geometry;

class ParquetReaderTest {
  private final PlanetilerConfig config = PlanetilerConfig.defaults();
  private static final String URL = "http://host/boston.parquet";
  private final Stats stats = Stats.inMemory();

  static List<Path> bostons() {
    return Glob.of(TestUtils.pathToResource("parquet")).resolve("boston*.parquet").find();
  }

  @ParameterizedTest
  @MethodSource("bostons")
  @Timeout(30)
  void testReadOvertureParquet(Path path) {
    List<String> ids = new CopyOnWriteArrayList<>();
    List<Geometry> geoms = new CopyOnWriteArrayList<>();

    var profile = new Profile.NullProfile() {
      volatile double height = 0;

      @Override
      public synchronized void processFeature(SourceFeature sourceFeature, FeatureCollector features) {
        try {
          ids.add(sourceFeature.getString("id"));
          height += Parse.parseDoubleOrNull(sourceFeature.getTag("height")) instanceof Double d ? d : 0;
          geoms.add(sourceFeature.latLonGeometry());
        } catch (GeometryException e) {
          throw new RuntimeException(e);
        }
      }
    };
    var reader = new ParquetReader("source", profile, stats);
    reader.process(List.of(path),
      FeatureGroup.newInMemoryFeatureGroup(TileOrder.TMS, profile, config, stats), PlanetilerConfig.defaults());
    assertEquals(List.of(
      "08b2a306638a0fff02001c5b97636c80",
      "08b2a306638a0fff0200a75c80c3d54b",
      "08b2a306638a0fff0200d1814977faca"
    ), ids.stream().sorted().toList());
    var center = GeoUtils.combine(geoms.toArray(Geometry[]::new)).getCentroid();
    assertEquals(newPoint(-71.07448, 42.35626), round(center));
    assertEquals(4.7, profile.height);
  }


  @Test
  void testHivePartitionFields() {
    assertNull(ParquetReader.getHivePartitionFields(Path.of("")));
    assertNull(ParquetReader.getHivePartitionFields(Path.of("a")));
    assertNull(ParquetReader.getHivePartitionFields(Path.of("a", "b")));
    assertEquals(Map.of("c", "d"), ParquetReader.getHivePartitionFields(Path.of("a", "b", "c=d")));
    assertEquals(Map.of("c", "d", "e", "f"),
      ParquetReader.getHivePartitionFields(Path.of("a", "b", "c=d", "e=f")));
    assertEquals(Map.of("a", "b", "c", "d", "e", "f"),
      ParquetReader.getHivePartitionFields(Path.of("a=b", "b", "c=d", "e=f")));
  }

  @Test
  @Timeout(30)
  void testNothingToRead() {
    // bounds that miss every file, and no files at all (e.g. Overture bounds over open ocean)
    var outsideBoston = PlanetilerConfig.from(Arguments.of("bounds", "7.40,43.72,7.44,43.75"));
    List<String> ids = new CopyOnWriteArrayList<>();
    var profile = new Profile.NullProfile() {
      @Override
      public void processFeature(SourceFeature sourceFeature, FeatureCollector features) {
        ids.add(sourceFeature.getString("id"));
      }
    };
    var featureGroup = FeatureGroup.newInMemoryFeatureGroup(TileOrder.TMS, profile, outsideBoston, stats);
    var reader = new ParquetReader("source", profile, stats);
    var files = bostons();

    reader.process(files, featureGroup, outsideBoston);
    reader.process(List.of(), featureGroup, outsideBoston);

    assertEquals(List.of(), ids);
  }

  @Test
  @Timeout(30)
  void testReadPercentComesFromBlocksWhenFilteredToBounds() {
    // the bounds filter leaves the number of features to read unknown until they have all been read
    var aroundBoston = PlanetilerConfig.from(Arguments.of("bounds", "-71.1,42.3,-71.0,42.4"));
    Profile profile = (SourceFeature source, FeatureCollector features) -> features.point("points");
    var featureGroup = FeatureGroup.newInMemoryFeatureGroup(TileOrder.TMS, profile, aroundBoston, stats);
    var source = new ParquetReader("source", profile, stats)
      .source(() -> List.of(bostons().getFirst()), CompletableFutures.NULL, aroundBoston);

    SourceFeatureProcessor.process("source", List.of(source), List.of(), featureGroup, aroundBoston, profile, stats);

    String log = TestUtils.withoutColors(ProgressLoggers.create().addSourceProgress(source.progress()).getLog());
    assertTrue(log.startsWith(" read: [    3 100% "), log);
  }

  @Test
  @Timeout(30)
  void testClosesFilesOpenedAheadThatNeverGetRead() throws IOException {
    var files = bostons();
    List<Path> closed = new CopyOnWriteArrayList<>();
    var source = new ParquetReader("source", new Profile.NullProfile(), stats).remoteSource(
      CompletableFuture.completedFuture(files.stream().collect(Collectors.toMap(path -> path, _ -> URL))),
      path -> recordingClose(RangeFile.openLocal(path), () -> closed.add(path)),
      new StubDownloader(config), config);
    source.whenReady().join();

    // like when a stage before this source fails
    source.close();

    assertEquals(Set.copyOf(files), Set.copyOf(closed));
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  @Timeout(30)
  void testRerunFetchesRangesMissingFromCache(boolean changedOnServer, @TempDir Path tempDir) throws IOException {
    byte[] bytes = Files.readAllBytes(bostons().getFirst());
    Path path = tempDir.resolve("boston.parquet");
    // bounds that miss the file leave only its footer in the cache
    var outsideBoston = PlanetilerConfig.from(Arguments.of("bounds", "7.40,43.72,7.44,43.75"));
    var firstRun = new StubDownloader(config).put(URL, bytes, "\"v1\"", null);
    assertEquals(List.of(), readCached(path, firstRun, outsideBoston));
    var downloader = new StubDownloader(config).put(URL, bytes, changedOnServer ? "\"v2\"" : "\"v1\"", null);

    var ids = readCached(path, downloader, config);

    assertEquals(3, ids.size());
    // starting over fetches the footer again with the request that opens the file
    var ranges = downloader.requests().stream().map(StubDownloader.Request::range).toList();
    assertEquals(changedOnServer, ranges.stream().anyMatch(range -> range.startsWith("bytes=-")), ranges::toString);
    // the next run has every range it needs, so it makes no requests
    var offline = new StubDownloader(config);
    assertEquals(ids, readCached(path, offline, config));
    assertEquals(List.of(), offline.requests());
  }

  private List<String> readCached(Path path, StubDownloader downloader, PlanetilerConfig config) {
    List<String> ids = new CopyOnWriteArrayList<>();
    Profile profile = (SourceFeature source, FeatureCollector features) -> ids.add(source.getString("id"));
    // The test files are smaller than the footer prefetch, so prefetch only the last 8 bytes to leave the row groups
    // out of the cache when bounds skip them.
    var source = new ParquetReader("source", profile, stats).remoteSource(
      CompletableFuture.completedFuture(Map.of(path, URL)),
      file -> RangeFile.open(file, URL, 8, DownloadMode.CACHED, false, downloader), downloader, config);
    var featureGroup = FeatureGroup.newInMemoryFeatureGroup(TileOrder.TMS, profile, config, stats);
    SourceFeatureProcessor.process("source", List.of(source), List.of(), featureGroup, config, profile, stats);
    return ids.stream().sorted().toList();
  }

  private static RangeFile recordingClose(RangeFile file, Runnable onClose) {
    return new RangeFile() {
      @Override
      public long length() throws IOException {
        return file.length();
      }

      @Override
      public byte[] read(long offset, int length) throws IOException {
        return file.read(offset, length);
      }

      @Override
      public void close() throws IOException {
        onClose.run();
        file.close();
      }
    };
  }
}
