package com.onthegomap.flatmap;

import static com.onthegomap.flatmap.TestUtils.assertSameJson;
import static com.onthegomap.flatmap.TestUtils.assertSubmap;
import static com.onthegomap.flatmap.TestUtils.feature;
import static com.onthegomap.flatmap.TestUtils.newPoint;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.graphhopper.reader.ReaderRelation;
import com.onthegomap.flatmap.collections.FeatureGroup;
import com.onthegomap.flatmap.collections.FeatureSort;
import com.onthegomap.flatmap.collections.LongLongMap;
import com.onthegomap.flatmap.geo.GeoUtils;
import com.onthegomap.flatmap.geo.TileCoord;
import com.onthegomap.flatmap.monitoring.Stats;
import com.onthegomap.flatmap.profiles.OpenMapTilesProfile;
import com.onthegomap.flatmap.read.OpenStreetMapReader;
import com.onthegomap.flatmap.read.Reader;
import com.onthegomap.flatmap.read.ReaderFeature;
import com.onthegomap.flatmap.worker.Topology;
import com.onthegomap.flatmap.write.Mbtiles;
import com.onthegomap.flatmap.write.MbtilesWriter;
import java.io.IOException;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Function;
import org.junit.jupiter.api.Test;

/**
 * In-memory tests with fake data and profiles to ensure all features work end-to-end.
 */
public class FlatMapTest {

  private static final String TEST_PROFILE_NAME = "test name";
  private static final String TEST_PROFILE_DESCRIPTION = "test description";
  private static final String TEST_PROFILE_ATTRIBUTION = "test attribution";
  private static final String TEST_PROFILE_VERSION = "test version";
  private static final int Z14_TILES = 1 << 14;
  private static final double Z14_WIDTH = 1d / Z14_TILES;
  private static final int Z13_TILES = 1 << 13;
  private static final double Z13_WIDTH = 1d / Z13_TILES;
  private final Stats stats = new Stats.InMemory();

  private void processReaderFeatures(FeatureGroup featureGroup, Profile profile, CommonParams config,
    List<? extends SourceFeature> features) {
    new Reader(profile, stats) {

      @Override
      public long getCount() {
        return features.size();
      }

      @Override
      public Topology.SourceStep<SourceFeature> read() {
        return features::forEach;
      }

      @Override
      public void close() {
      }
    }.process(
      "test", featureGroup, config
    );
  }

  private FlatMapResults runWithReaderFeatures(
    Map<String, String> args,
    List<ReaderFeature> features,
    BiConsumer<SourceFeature, FeatureCollector> profileFunction
  ) throws IOException, SQLException {
    CommonParams config = CommonParams.from(Arguments.of(args));
    var translations = Translations.defaultProvider(List.of());
    var profile1 = new OpenMapTilesProfile();
    LongLongMap nodeLocations = LongLongMap.newInMemorySortedTable();
    FeatureSort featureDb = FeatureSort.newInMemory();
    FeatureGroup featureGroup = new FeatureGroup(featureDb, profile1, stats);
    var profile = TestProfile.processSourceFeatures(profileFunction);
    processReaderFeatures(featureGroup, profile, config, features);
    featureGroup.sorter().sort();
    try (Mbtiles db = Mbtiles.newInMemoryDatabase()) {
      MbtilesWriter.writeOutput(featureGroup, db, () -> 0L, profile, config, stats);
      return new FlatMapResults(TestUtils.getTileMap(db), db.metadata().getAll());
    }
  }

  @Test
  public void testMetadataButNoPoints() throws IOException, SQLException {
    var results = runWithReaderFeatures(
      Map.of("threads", "1"),
      List.of(),
      (sourceFeature, features) -> {
      }
    );
    assertEquals(Map.of(), results.tiles);
    assertSubmap(Map.of(
      "name", TEST_PROFILE_NAME,
      "description", TEST_PROFILE_DESCRIPTION,
      "attribution", TEST_PROFILE_ATTRIBUTION,
      "version", TEST_PROFILE_VERSION,
      "type", "baselayer",
      "format", "pbf",
      "minzoom", "0",
      "maxzoom", "14",
      "center", "0,0,0",
      "bounds", "-180,-85.05113,180,85.05113"
    ), results.metadata);
    assertSameJson(
      """
        {
          "vector_layers": [
          ]
        }
        """,
      results.metadata.get("json")
    );
  }

  @Test
  public void testSinglePoint() throws IOException, SQLException {
    double x = 0.5 + Z14_WIDTH / 2;
    double y = 0.5 + Z14_WIDTH / 2;
    double lat = GeoUtils.getWorldLat(y);
    double lng = GeoUtils.getWorldLon(x);

    var results = runWithReaderFeatures(
      Map.of("threads", "1"),
      List.of(
        new ReaderFeature(newPoint(lng, lat), Map.of(
          "attr", "value"
        ))
      ),
      (in, features) -> {
        features.point("layer")
          .setZoomRange(13, 14)
          .setAttr("name", "name value")
          .inheritFromSource("attr");
      }
    );

    assertSubmap(Map.of(
      TileCoord.ofXYZ(Z14_TILES / 2, Z14_TILES / 2, 14), List.of(
        feature(newPoint(128, 128), Map.of(
          "attr", "value",
          "name", "name value"
        ))
      ),
      TileCoord.ofXYZ(Z13_TILES / 2, Z13_TILES / 2, 13), List.of(
        feature(newPoint(64, 64), Map.of(
          "attr", "value",
          "name", "name value"
        ))
      )
    ), results.tiles);
    assertSameJson(
      """
        {
          "vector_layers": [
            {"id": "layer", "fields": {"name": "String", "attr": "String"}, "minzoom": 13, "maxzoom": 14}
          ]
        }
        """,
      results.metadata.get("json")
    );
  }

  @Test
  public void testLabelGridLimit() throws IOException, SQLException {
    double y = 0.5 + Z14_WIDTH / 2;
    double lat = GeoUtils.getWorldLat(y);

    double x1 = 0.5 + Z14_WIDTH / 4;
    double lng1 = GeoUtils.getWorldLon(x1);
    double lng2 = GeoUtils.getWorldLon(x1 + Z14_WIDTH * 10d / 256);
    double lng3 = GeoUtils.getWorldLon(x1 + Z14_WIDTH * 20d / 256);

    var results = runWithReaderFeatures(
      Map.of("threads", "1"),
      List.of(
        new ReaderFeature(newPoint(lng1, lat), Map.of("rank", "1")),
        new ReaderFeature(newPoint(lng2, lat), Map.of("rank", "2")),
        new ReaderFeature(newPoint(lng3, lat), Map.of("rank", "3"))
      ),
      (in, features) -> {
        features.point("layer")
          .setZoomRange(13, 14)
          .inheritFromSource("rank")
          .setZorder(Integer.parseInt(in.getTag("rank").toString()))
          .setLabelGridSizeAndLimit(13, 128, 2);
      }
    );

    assertSubmap(Map.of(
      TileCoord.ofXYZ(Z14_TILES / 2, Z14_TILES / 2, 14), List.of(
        feature(newPoint(64, 128), Map.of("rank", "1")),
        feature(newPoint(74, 128), Map.of("rank", "2")),
        feature(newPoint(84, 128), Map.of("rank", "3"))
      ),
      TileCoord.ofXYZ(Z13_TILES / 2, Z13_TILES / 2, 13), List.of(
        // omit rank=1 due to label grid size
        feature(newPoint(37, 64), Map.of("rank", "2")),
        feature(newPoint(42, 64), Map.of("rank", "3"))
      )
    ), results.tiles);
  }

  private interface LayerPostprocessFunction {

    List<VectorTileEncoder.Feature> process(String layer, int zoom, List<VectorTileEncoder.Feature> items);
  }

  private static record FlatMapResults(
    Map<TileCoord, List<TestUtils.ComparableFeature>> tiles, Map<String, String> metadata
  ) {

  }

  private static record TestProfile(
    @Override String name,
    @Override String description,
    @Override String attribution,
    @Override String version,
    BiConsumer<SourceFeature, FeatureCollector> processFeature,
    Function<ReaderRelation, List<OpenStreetMapReader.RelationInfo>> preprocessOsmRelation,
    LayerPostprocessFunction postprocessLayerFeatures
  ) implements Profile {

    TestProfile(
      BiConsumer<SourceFeature, FeatureCollector> processFeature,
      Function<ReaderRelation, List<OpenStreetMapReader.RelationInfo>> preprocessOsmRelation,
      LayerPostprocessFunction postprocessLayerFeatures
    ) {
      this(TEST_PROFILE_NAME, TEST_PROFILE_DESCRIPTION, TEST_PROFILE_ATTRIBUTION, TEST_PROFILE_VERSION, processFeature,
        preprocessOsmRelation,
        postprocessLayerFeatures);
    }

    static TestProfile processSourceFeatures(BiConsumer<SourceFeature, FeatureCollector> processFeature) {
      return new TestProfile(processFeature, (a) -> null, (a, b, c) -> null);
    }

    @Override
    public List<OpenStreetMapReader.RelationInfo> preprocessOsmRelation(
      ReaderRelation relation) {
      return preprocessOsmRelation.apply(relation);
    }

    @Override
    public void processFeature(SourceFeature sourceFeature, FeatureCollector features) {
      processFeature.accept(sourceFeature, features);
    }

    @Override
    public void release() {
    }

    @Override
    public List<VectorTileEncoder.Feature> postProcessLayerFeatures(String layer, int zoom,
      List<VectorTileEncoder.Feature> items) {
      return postprocessLayerFeatures.process(layer, zoom, items);
    }
  }
}
