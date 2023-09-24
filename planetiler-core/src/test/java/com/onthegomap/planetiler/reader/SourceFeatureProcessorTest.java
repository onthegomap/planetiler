package com.onthegomap.planetiler.reader;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.onthegomap.planetiler.Profile;
import com.onthegomap.planetiler.collection.FeatureGroup;
import com.onthegomap.planetiler.config.PlanetilerConfig;
import com.onthegomap.planetiler.geo.GeoUtils;
import com.onthegomap.planetiler.geo.TileOrder;
import com.onthegomap.planetiler.stats.Stats;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

class SourceFeatureProcessorTest {

  private static class MockReader extends SimpleReader<SimpleFeature> {

    private final Path path;
    private final List<SimpleFeature> emittedFeatures;

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
    public void close() {}
  }

  @Test
  void testCountFeatures() {
    var paths = List.of(
      Path.of("1"),
      Path.of("22"),
      Path.of("333")
    );

    var processor = new SourceFeatureProcessor<>(
      "sourceName",
      path -> new MockReader(new ArrayList<>(), path, "sourceName"),
      new Profile.NullProfile(),
      Stats.inMemory()
    );

    assertEquals(6, processor.getFeatureCount(paths));
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
}
