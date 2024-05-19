package com.onthegomap.planetiler.reader.parquet;

import static com.onthegomap.planetiler.TestUtils.newPoint;
import static com.onthegomap.planetiler.TestUtils.round;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import com.onthegomap.planetiler.FeatureCollector;
import com.onthegomap.planetiler.Profile;
import com.onthegomap.planetiler.TestUtils;
import com.onthegomap.planetiler.collection.FeatureGroup;
import com.onthegomap.planetiler.config.PlanetilerConfig;
import com.onthegomap.planetiler.geo.GeoUtils;
import com.onthegomap.planetiler.geo.GeometryException;
import com.onthegomap.planetiler.geo.TileOrder;
import com.onthegomap.planetiler.reader.SourceFeature;
import com.onthegomap.planetiler.stats.Stats;
import com.onthegomap.planetiler.util.Glob;
import com.onthegomap.planetiler.util.Parse;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.locationtech.jts.geom.Geometry;

class ParquetReaderTest {
  private final PlanetilerConfig config = PlanetilerConfig.defaults();
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
}
