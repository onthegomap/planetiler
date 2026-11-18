package com.onthegomap.planetiler.reader;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.onthegomap.planetiler.FeatureCollector;
import com.onthegomap.planetiler.Profile;
import com.onthegomap.planetiler.collection.FeatureGroup;
import com.onthegomap.planetiler.config.PlanetilerConfig;
import com.onthegomap.planetiler.geo.GeoUtils;
import com.onthegomap.planetiler.geo.TileOrder;
import com.onthegomap.planetiler.reader.shapefile.TestShapefileWriter;
import com.onthegomap.planetiler.stats.Stats;
import com.onthegomap.planetiler.util.CompletableFutures;
import com.onthegomap.planetiler.util.Exceptions;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

class SourceFeatureProcessorTest {

  private static class MockReader extends SimpleReader<SimpleFeature> {

    private final Path path;
    private final List<SimpleFeature> emittedFeatures;
    private boolean closed = false;

    public MockReader(List<SimpleFeature> features, Path path, String sourceName) {
      super(sourceName);
      this.path = path;
      this.emittedFeatures = features;
    }

    @Override
    public long getFeatureCount() {
      return path.toString().length();
    }

    @Override
    public void readFeatures(Consumer<SimpleFeature> next) {
      var feature = SimpleFeature.create(GeoUtils.EMPTY_POINT, Map.of(), sourceName, path.toString(), 0);

      next.accept(feature);
      emittedFeatures.add(feature);
    }

    @Override
    public void close() {
      closed = true;
    }
  }

  @Test
  void testProcessMultipleInputs() {
    var profile = new Profile.NullProfile();
    var stats = Stats.inMemory();
    var config = PlanetilerConfig.defaults();
    var featureGroup = FeatureGroup.newInMemoryFeatureGroup(TileOrder.TMS, profile, config, stats);

    var emittedFeatures = new ArrayList<SimpleFeature>();
    var paths = List.of(
      Path.of("a"),
      Path.of("b"),
      Path.of("c")
    );

    var processor = new SourceFeatureProcessor<>(
      "sourceName",
      path -> new MockReader(emittedFeatures, path, "sourceName"),
      profile,
      stats
    );

    processor.processFiles(paths, featureGroup, PlanetilerConfig.defaults());

    assertEquals(3, emittedFeatures.size());
    assertEquals(
      Set.of("a", "b", "c"),
      emittedFeatures.stream().map(SourceFeature::getSourceLayer).collect(Collectors.toSet()));
  }

  @Test
  void testUsesOneReaderPerFileToCountAndRead() {
    var profile = new Profile.NullProfile();
    var stats = Stats.inMemory();
    var config = PlanetilerConfig.defaults();
    var featureGroup = FeatureGroup.newInMemoryFeatureGroup(TileOrder.TMS, profile, config, stats);
    List<MockReader> readers = new ArrayList<>();
    var processor = new SourceFeatureProcessor<>(
      "sourceName",
      path -> {
        var reader = new MockReader(new ArrayList<>(), path, "sourceName");
        readers.add(reader);
        return reader;
      },
      profile,
      stats
    );

    processor.processFiles(List.of(Path.of("a"), Path.of("b")), featureGroup, config);

    assertEquals(2, readers.size());
    assertTrue(readers.stream().allMatch(reader -> reader.closed));
    assertEquals(List.of(1, 1), readers.stream().map(reader -> reader.emittedFeatures.size()).toList());
  }

  @Test
  void testSkipsFeatureWhoseGeometryCantBeRead() {
    var stats = Stats.inMemory();
    var config = PlanetilerConfig.defaults();
    List<String> processed = new CopyOnWriteArrayList<>();
    Profile profile = (SourceFeature source, FeatureCollector features) -> processed.add(source.getSourceLayer());
    // a polygon record that ends right after its shape type
    var unreadable = TestShapefileWriter.unreadableFeature("unreadable", 1);
    var readable = SimpleFeature.create(GeoUtils.point(1, 2), Map.of(), "sourceName", "readable", 2);
    var processor = new SourceFeatureProcessor<SourceFeature>("sourceName", path -> new SimpleReader<>("sourceName") {
      @Override
      public long getFeatureCount() {
        return 2;
      }

      @Override
      public void readFeatures(Consumer<SourceFeature> next) {
        next.accept(unreadable);
        next.accept(readable);
      }

      @Override
      public void close() {
        // nothing to close
      }
    }, profile, stats);

    processor.processFiles(List.of(Path.of("a")),
      FeatureGroup.newInMemoryFeatureGroup(TileOrder.TMS, profile, config, stats), config);

    assertEquals(List.of("readable"), processed);
  }

  @Test
  void testSkipsBoundsCheckIfReaderFiltersToBounds() {
    var stats = Stats.inMemory();
    var config = PlanetilerConfig.defaults();
    List<String> processed = new CopyOnWriteArrayList<>();
    Profile profile = (SourceFeature source, FeatureCollector features) -> processed.add(source.getSourceLayer());
    // the check would throw reading this geometry, so the profile only sees it if the check is skipped
    var unreadable = TestShapefileWriter.unreadableFeature("unreadable", 1);
    var processor = new SourceFeatureProcessor<SourceFeature>("sourceName", path -> new SimpleReader<>("sourceName") {
      @Override
      public long getFeatureCount() {
        return 1;
      }

      @Override
      public boolean filtersToBounds() {
        return true;
      }

      @Override
      public void readFeatures(Consumer<SourceFeature> next) {
        next.accept(unreadable);
      }

      @Override
      public void close() {
        // nothing to close
      }
    }, profile, stats);

    processor.processFiles(List.of(Path.of("a")),
      FeatureGroup.newInMemoryFeatureGroup(TileOrder.TMS, profile, config, stats), config);

    assertEquals(List.of("unreadable"), processed);
  }

  @Test
  void testCountsWhatEachSourceReadAndWrote() {
    var stats = Stats.inMemory();
    var config = PlanetilerConfig.defaults();
    Profile profile = (SourceFeature source, FeatureCollector features) -> features.point("points").setZoomRange(0, 0);
    var featureGroup = FeatureGroup.newInMemoryFeatureGroup(TileOrder.TMS, profile, config, stats);
    var a = pointsSource("a", 2, CompletableFutures.NULL, config);
    var b = pointsSource("b", 3, CompletableFutures.NULL, config);

    SourceFeatureProcessor.process("sources", List.of(a, b), List.of(), featureGroup, config, profile, stats);

    assertTrue(a.progress().summary().matches("read: 2 in .*  process cpu:.*  written: 2"), a.progress()::summary);
    assertTrue(b.progress().summary().matches("read: 3 in .*  process cpu:.*  written: 3"), b.progress()::summary);
  }

  @Test
  void testRethrowsDownloadFailure() {
    var stats = Stats.inMemory();
    var config = PlanetilerConfig.defaults();
    var profile = new Profile.NullProfile();
    var featureGroup = FeatureGroup.newInMemoryFeatureGroup(TileOrder.TMS, profile, config, stats);
    var ready = pointsSource("ready", 1, CompletableFutures.NULL, config);
    var failed = pointsSource("failed", 1, CompletableFuture.failedFuture(new IOException("download failed")), config);
    var sources = List.of(ready, failed);

    var exception = assertThrows(RuntimeException.class,
      () -> SourceFeatureProcessor.process("sources", sources, List.of(), featureGroup, config, profile, stats));
    assertEquals("download failed", Exceptions.unwrap(exception).getMessage());
  }

  private static SourceReader pointsSource(String name, int count, CompletableFuture<Void> ready,
    PlanetilerConfig config) {
    return SourceReader.ofFiles(name, () -> List.of(Path.of(name)), path -> new SimpleReader<SourceFeature>(name) {
      @Override
      public long getFeatureCount() {
        return count;
      }

      @Override
      public void readFeatures(Consumer<SourceFeature> next) {
        for (int i = 0; i < count; i++) {
          next.accept(SimpleFeature.create(GeoUtils.point(0, 0), Map.of(), name, null, i));
        }
      }

      @Override
      public void close() {
        // nothing to close
      }
    }, ready, config.bounds());
  }
}
