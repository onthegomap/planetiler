package com.onthegomap.flatmap;

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
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.zip.GZIPInputStream;
import org.junit.jupiter.api.Test;

public class FeatureTest {

  private Stats stats = new Stats.InMemory();

  private static record FlatMapComponents(
    FeatureGroup featureGroup, FeatureRenderer featureRenderer, Translations translations
  ) {

  }

  private FlatMapComponents newInMemoryFlatmapRunner(CommonParams config) {
    var translations = Translations.defaultProvider(List.of());
    var profile = new OpenMapTilesProfile();
    LongLongMap nodeLocations = LongLongMap.newInMemorySortedTable();
    FeatureSort featureDb = FeatureSort.newInMemory();
    FeatureGroup featureGroup = new FeatureGroup(featureDb, profile);
    FeatureRenderer renderer = new FeatureRenderer(config);
    return new FlatMapComponents(featureGroup, renderer, translations);
  }

  private void processReaderFeatures(FlatMapComponents flatmap, Profile profile, CommonParams config,
    List<SourceFeature> features) {
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
      "test", flatmap.featureRenderer, flatmap.featureGroup, config
    );
  }

  private static List<VectorTileEncoder.Feature> decode(byte[] compressed) {
    var bis = new ByteArrayInputStream(compressed);
    try (var gzipIS = new GZIPInputStream(bis)) {
      byte[] decompressed = gzipIS.readAllBytes();
      return VectorTileEncoder.decode(decompressed);
    } catch (IOException e) {
      throw new IllegalStateException("Unable to decode compressed tile", e);
    }
  }

  private interface LayerPostprocessFunction {

    List<VectorTileEncoder.Feature> process(String layer, int zoom, List<VectorTileEncoder.Feature> items);
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
      this("test name", "test description", "test attribution", "test version", processFeature, preprocessOsmRelation,
        postprocessLayerFeatures);
    }

    static TestProfile processSourceFeatures(BiConsumer<SourceFeature, FeatureCollector> processFeature) {
      return new TestProfile(processFeature, (a) -> null, (a, b, c) -> null);
    }

    static TestProfile noop() {
      return processSourceFeatures((a, b) -> {
      });
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

  @Test
  public void testMetadataButNoPoints() throws IOException, SQLException {
    CommonParams config = CommonParams.from(Arguments.of(
      "threads", "1"
    ));
    var flatmap = newInMemoryFlatmapRunner(config);
    var profile = TestProfile.noop();
    flatmap.featureGroup.sorter().sort();
    try (Mbtiles db = Mbtiles.newInMemoryDatabase()) {
      MbtilesWriter.writeOutput(flatmap.featureGroup, db, () -> 0L, profile, config, stats);
      assertEquals(Set.of(), TestUtils.getAllTiles(db));
      assertSubmap(Map.of(
        "name", profile.name,
        "description", profile.description,
        "attribution", profile.attribution,
        "version", profile.version,
        "type", "baselayer",
        "format", "pbf",
        "minzoom", "0",
        "maxzoom", "14",
        "center", "0,0,0",
        "bounds", "-180,-85.05113,180,85.05113"
      ), db.metadata().getAll());
    }
  }

  private static final int Z14_TILES = 1 << 14;
  private static final double Z14_WIDTH = 1d / Z14_TILES;

  @Test
  public void testSinglePoint() throws IOException, SQLException {
    CommonParams config = CommonParams.from(Arguments.of(
      "threads", "1"
    ));
    var flatmap = newInMemoryFlatmapRunner(config);
    var profile = TestProfile.processSourceFeatures((in, features) -> {
      features.point("layer")
        .zoomRange(14, 14)
        .setAttr("name", "name value")
        .inheritFromSource("attr");
    });

    double x = 0.5 + Z14_WIDTH / 2;
    double y = 0.5 + Z14_WIDTH / 2;
    double lat = GeoUtils.getWorldLat(y);
    double lng = GeoUtils.getWorldLon(x);

    processReaderFeatures(flatmap, profile, config, List.of(
      new ReaderFeature(newPoint(lng, lat), Map.of(
        "attr", "value"
      ))
    ));

    flatmap.featureGroup.sorter().sort();

    try (Mbtiles db = Mbtiles.newInMemoryDatabase()) {
      MbtilesWriter.writeOutput(flatmap.featureGroup, db, () -> 0L, profile, config, stats);
      assertSubmap(Map.of(
        TileCoord.ofXYZ(Z14_TILES / 2, Z14_TILES / 2, 14), List.of(
          feature(newPoint(128, 128), Map.of(
            "attr", "value",
            "name", "name value"
          ))
        )
      ), TestUtils.getTileMap(db));
    }
  }

  // TODO: refactor into parameterized test?
}
