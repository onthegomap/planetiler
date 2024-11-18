package com.onthegomap.planetiler;

import static com.onthegomap.planetiler.TestUtils.*;
import static com.onthegomap.planetiler.collection.FeatureGroup.SORT_KEY_BITS;
import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import com.onthegomap.planetiler.archive.ReadableTileArchive;
import com.onthegomap.planetiler.archive.TileArchiveConfig;
import com.onthegomap.planetiler.archive.TileArchiveMetadata;
import com.onthegomap.planetiler.archive.TileArchiveWriter;
import com.onthegomap.planetiler.archive.TileArchives;
import com.onthegomap.planetiler.archive.TileCompression;
import com.onthegomap.planetiler.collection.FeatureGroup;
import com.onthegomap.planetiler.collection.LongLongMap;
import com.onthegomap.planetiler.collection.LongLongMultimap;
import com.onthegomap.planetiler.config.Arguments;
import com.onthegomap.planetiler.config.PlanetilerConfig;
import com.onthegomap.planetiler.files.ReadableFilesArchive;
import com.onthegomap.planetiler.geo.GeoUtils;
import com.onthegomap.planetiler.geo.GeometryException;
import com.onthegomap.planetiler.geo.GeometryType;
import com.onthegomap.planetiler.geo.TileCoord;
import com.onthegomap.planetiler.geo.TileOrder;
import com.onthegomap.planetiler.layers.Poi;
import com.onthegomap.planetiler.mbtiles.Mbtiles;
import com.onthegomap.planetiler.pmtiles.ReadablePmtiles;
import com.onthegomap.planetiler.reader.SimpleFeature;
import com.onthegomap.planetiler.reader.SimpleReader;
import com.onthegomap.planetiler.reader.SourceFeature;
import com.onthegomap.planetiler.reader.SourceFeatureProcessor;
import com.onthegomap.planetiler.reader.osm.OsmBlockSource;
import com.onthegomap.planetiler.reader.osm.OsmElement;
import com.onthegomap.planetiler.reader.osm.OsmReader;
import com.onthegomap.planetiler.reader.osm.OsmRelationInfo;
import com.onthegomap.planetiler.stats.Stats;
import com.onthegomap.planetiler.stream.InMemoryStreamArchive;
import com.onthegomap.planetiler.util.BuildInfo;
import com.onthegomap.planetiler.util.Gzip;
import com.onthegomap.planetiler.util.JsonUitls;
import com.onthegomap.planetiler.util.SortKey;
import com.onthegomap.planetiler.util.TileSizeStats;
import com.onthegomap.planetiler.util.ZoomFunction;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.DoubleStream;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.junit.platform.commons.util.StringUtils;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.MultiPolygon;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.io.InputStreamInStream;
import org.locationtech.jts.io.WKBReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * In-memory tests with fake data and profiles to ensure all features work end-to-end.
 */
class PlanetilerTests {

  private static final Logger LOGGER = LoggerFactory.getLogger(PlanetilerTests.class);

  private static final String TEST_PROFILE_NAME = "test name";
  private static final String TEST_PROFILE_DESCRIPTION = "test description";
  private static final String TEST_PROFILE_ATTRIBUTION = "test attribution";
  private static final String TEST_PROFILE_VERSION = "test version";
  private static final int Z15_TILES = 1 << 15;
  private static final double Z15_WIDTH = 1d / Z15_TILES;
  private static final int Z14_TILES = 1 << 14;
  private static final double Z14_WIDTH = 1d / Z14_TILES;
  private static final int Z13_TILES = 1 << 13;
  private static final double Z13_WIDTH = 1d / Z13_TILES;
  private static final int Z12_TILES = 1 << 12;
  private static final double Z12_WIDTH = 1d / Z12_TILES;
  private static final int Z11_TILES = 1 << 11;
  private static final double Z11_WIDTH = 1d / Z11_TILES;
  private static final int Z4_TILES = 1 << 4;
  private static final Polygon WORLD_POLYGON = newPolygon(
    worldCoordinateList(
      Z14_WIDTH / 2, Z14_WIDTH / 2,
      1 - Z14_WIDTH / 2, Z14_WIDTH / 2,
      1 - Z14_WIDTH / 2, 1 - Z14_WIDTH / 2,
      Z14_WIDTH / 2, 1 - Z14_WIDTH / 2,
      Z14_WIDTH / 2, Z14_WIDTH / 2
    ),
    List.of()
  );
  private final Stats stats = Stats.inMemory();

  @TempDir
  Path tempDir;

  private static <T extends OsmElement> T with(T elem, Consumer<T> fn) {
    fn.accept(elem);
    return elem;
  }

  private <F extends SourceFeature> void processReaderFeatures(FeatureGroup featureGroup, Profile profile,
    PlanetilerConfig config, List<F> features) {
    SourceFeatureProcessor.processFiles(
      "test",
      List.of(Path.of("mock-path")), path -> new SimpleReader<F>("test") {
        @Override
        public long getFeatureCount() {
          return features.size();
        }

        @Override
        public void readFeatures(Consumer<F> next) {
          features.forEach(next);
        }

        @Override
        public void close() { /* pass */ }
      }, featureGroup, config, profile, stats
    );
  }

  private void processOsmFeatures(FeatureGroup featureGroup, Profile profile, PlanetilerConfig config,
    List<? extends OsmElement> osmElements) throws IOException {
    OsmBlockSource elems = next -> {
      // process the same order they come in from an OSM file
      next.accept(OsmBlockSource.Block.of(osmElements.stream().filter(e -> e instanceof OsmElement.Other).toList()));
      next.accept(OsmBlockSource.Block.of(osmElements.stream().filter(e -> e instanceof OsmElement.Node).toList()));
      next.accept(OsmBlockSource.Block.of(osmElements.stream().filter(e -> e instanceof OsmElement.Way).toList()));
      next.accept(OsmBlockSource.Block.of(osmElements.stream().filter(e -> e instanceof OsmElement.Relation).toList()));
    };
    var nodeMap = LongLongMap.newInMemorySortedTable();
    var multipolygons = LongLongMultimap.newInMemoryReplaceableMultimap();
    try (var reader = new OsmReader("osm", () -> elems, nodeMap, multipolygons, profile, Stats.inMemory())) {
      reader.pass1(config);
      reader.pass2(featureGroup, config);
    }
  }

  private PlanetilerResults run(
    Map<String, String> args,
    Runner runner,
    Profile profile
  ) throws Exception {
    PlanetilerConfig config = PlanetilerConfig.from(Arguments.of(args));
    FeatureGroup featureGroup = FeatureGroup.newInMemoryFeatureGroup(TileOrder.TMS, profile, config, stats);
    runner.run(featureGroup, profile, config);
    featureGroup.prepare();
    try (Mbtiles db = Mbtiles.newInMemoryDatabase(config.arguments())) {
      TileArchiveWriter.writeOutput(featureGroup, db, () -> 0L, new TileArchiveMetadata(profile, config),
        null, config, stats);
      var tileMap = TestUtils.getTileMap(db);
      tileMap.values().forEach(fs -> fs.forEach(f -> f.geometry().validate()));
      int tileDataCount = db.compactDb() ? TestUtils.getTilesDataCount(db) : 0;
      return new PlanetilerResults(tileMap, db.metadata().toMap(), tileDataCount);
    }
  }

  private PlanetilerResults runWithReaderFeatures(
    Map<String, String> args,
    List<SimpleFeature> features,
    BiConsumer<SourceFeature, FeatureCollector> profileFunction
  ) throws Exception {
    return run(
      args,
      (featureGroup, profile, config) -> processReaderFeatures(featureGroup, profile, config, features),
      TestProfile.processSourceFeatures(profileFunction)
    );
  }

  private PlanetilerResults runWithReaderFeatures(
    Map<String, String> args,
    List<SimpleFeature> features,
    BiConsumer<SourceFeature, FeatureCollector> profileFunction,
    LayerPostprocessFunction postProcess
  ) throws Exception {
    return run(
      args,
      (featureGroup, profile, config) -> processReaderFeatures(featureGroup, profile, config, features),
      new TestProfile(profileFunction, a -> null, postProcess)
    );
  }

  private PlanetilerResults runWithReaderFeaturesProfile(
    Map<String, String> args,
    List<SimpleFeature> features,
    Profile profileToUse
  ) throws Exception {
    return run(
      args,
      (featureGroup, profile, config) -> processReaderFeatures(featureGroup, profile, config, features),
      profileToUse
    );
  }


  private PlanetilerResults runWithOsmElements(
    Map<String, String> args,
    List<OsmElement> features,
    Profile profileToUse
  ) throws Exception {
    return run(
      args,
      (featureGroup, profile, config) -> processOsmFeatures(featureGroup, profile, config, features),
      profileToUse
    );
  }

  private PlanetilerResults runWithOsmElements(
    Map<String, String> args,
    List<OsmElement> features,
    Function<OsmElement.Relation, List<OsmRelationInfo>> preprocessOsmRelation,
    BiConsumer<SourceFeature, FeatureCollector> profileFunction
  ) throws Exception {
    return run(
      args,
      (featureGroup, profile, config) -> processOsmFeatures(featureGroup, profile, config, features),
      new TestProfile(profileFunction, preprocessOsmRelation, (a, b, c) -> c)
    );
  }

  private SimpleFeature newReaderFeature(Geometry geometry, Map<String, Object> attrs) {
    return SimpleFeature.create(geometry, attrs);
  }

  @Test
  void testMetadataButNoPoints() throws Exception {
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
      "center", "0,0",
      "bounds", "-180,-85.05113,180,85.05113"
    ), results.metadata);
    assertSubmap(Map.of(
      "planetiler:version", BuildInfo.get().version()
    ), results.metadata);
    assertSameJson(
      "[]",
      results.metadata.get("vector_layers")
    );
  }

  @Test
  void testOverrideMetadata() throws Exception {
    var results = runWithReaderFeatures(
      Map.of(
        "archive_name", "override_name",
        "archive_description", "override_description",
        "archive_attribution", "override_attribution",
        "archive_version", "override_version",
        "archive_type", "override_type"
      ),
      List.of(),
      (sourceFeature, features) -> {
      }
    );
    assertEquals(Map.of(), results.tiles);
    assertSubmap(Map.of(
      "name", "override_name",
      "description", "override_description",
      "attribution", "override_attribution",
      "version", "override_version",
      "type", "override_type"
    ), results.metadata);
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void testSinglePoint(boolean anyGeom) throws Exception {
    double x = 0.5 + Z14_WIDTH / 4;
    double y = 0.5 + Z14_WIDTH / 4;
    double lat = GeoUtils.getWorldLat(y);
    double lng = GeoUtils.getWorldLon(x);

    var results = runWithReaderFeatures(
      Map.of("threads", "1", "maxzoom", "15"),
      List.of(
        newReaderFeature(newPoint(lng, lat), Map.of(
          "attr", "value"
        ))
      ),
      (in, features) -> (anyGeom ? features.anyGeometry("layer") : features.point("layer"))
        .setZoomRange(13, 15)
        .setAttr("name", "name value")
        .inheritAttrFromSource("attr")
    );

    assertSubmap(Map.of(
      TileCoord.ofXYZ(Z15_TILES / 2, Z15_TILES / 2, 15), List.of(
        feature(newPoint(128, 128), Map.of(
          "attr", "value",
          "name", "name value"
        ))
      ),
      TileCoord.ofXYZ(Z14_TILES / 2, Z14_TILES / 2, 14), List.of(
        feature(newPoint(64, 64), Map.of(
          "attr", "value",
          "name", "name value"
        ))
      ),
      TileCoord.ofXYZ(Z13_TILES / 2, Z13_TILES / 2, 13), List.of(
        feature(newPoint(32, 32), Map.of(
          "attr", "value",
          "name", "name value"
        ))
      )
    ), results.tiles);
    assertSameJson(
      """
        [
          {"id": "layer", "fields": {"name": "String", "attr": "String"}, "minzoom": 13, "maxzoom": 15}
        ]
        """,
      results.metadata.get("vector_layers")
    );
  }

  @Test
  void testMultiPoint() throws Exception {
    double x1 = 0.5 + Z14_WIDTH / 2;
    double y1 = 0.5 + Z14_WIDTH / 2;
    double x2 = x1 + Z13_WIDTH / 256d;
    double y2 = y1 + Z13_WIDTH / 256d;
    double lat1 = GeoUtils.getWorldLat(y1);
    double lng1 = GeoUtils.getWorldLon(x1);
    double lat2 = GeoUtils.getWorldLat(y2);
    double lng2 = GeoUtils.getWorldLon(x2);

    var results = runWithReaderFeatures(
      Map.of("threads", "1"),
      List.of(
        newReaderFeature(newMultiPoint(
          newPoint(lng1, lat1),
          newPoint(lng2, lat2)
        ), Map.of(
          "attr", "value"
        ))
      ),
      (in, features) -> features.point("layer")
        .setZoomRange(13, 14)
        .setAttr("name", "name value")
        .inheritAttrFromSource("attr")
    );

    assertSubmap(Map.of(
      TileCoord.ofXYZ(Z14_TILES / 2, Z14_TILES / 2, 14), List.of(
        feature(newMultiPoint(
          newPoint(128, 128),
          newPoint(130, 130)
        ), Map.of(
          "attr", "value",
          "name", "name value"
        ))
      ),
      TileCoord.ofXYZ(Z13_TILES / 2, Z13_TILES / 2, 13), List.of(
        feature(newMultiPoint(
          newPoint(64, 64),
          newPoint(65, 65)
        ), Map.of(
          "attr", "value",
          "name", "name value"
        ))
      )
    ), results.tiles);
  }

  @Test
  void testLabelGridLimit() throws Exception {
    double y = 0.5 + Z14_WIDTH / 2;
    double lat = GeoUtils.getWorldLat(y);

    double x1 = 0.5 + Z14_WIDTH / 4;
    double lng1 = GeoUtils.getWorldLon(x1);
    double lng2 = GeoUtils.getWorldLon(x1 + Z14_WIDTH * 10d / 256);
    double lng3 = GeoUtils.getWorldLon(x1 + Z14_WIDTH * 20d / 256);

    var results = runWithReaderFeatures(
      Map.of("threads", "1"),
      List.of(
        newReaderFeature(newPoint(lng1, lat), Map.of("rank", "1")),
        newReaderFeature(newPoint(lng2, lat), Map.of("rank", "2")),
        newReaderFeature(newPoint(lng3, lat), Map.of("rank", "3"))
      ),
      (in, features) -> features.point("layer")
        .setZoomRange(13, 14)
        .inheritAttrFromSource("rank")
        .setSortKey(Integer.parseInt(in.getTag("rank").toString()))
        .setPointLabelGridSizeAndLimit(13, 128, 2)
        .setBufferPixels(128)
    );

    assertSubmap(Map.of(
      TileCoord.ofXYZ(Z14_TILES / 2, Z14_TILES / 2, 14), List.of(
        feature(newPoint(64, 128), Map.of("rank", "1")),
        feature(newPoint(74, 128), Map.of("rank", "2")),
        feature(newPoint(84, 128), Map.of("rank", "3"))
      ),
      TileCoord.ofXYZ(Z13_TILES / 2, Z13_TILES / 2, 13), List.of(
        // omit rank=3 due to label grid size
        feature(newPoint(32, 64), Map.of("rank", "1")),
        feature(newPoint(37, 64), Map.of("rank", "2"))
      )
    ), results.tiles);
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void testLineString(boolean anyGeom) throws Exception {
    double x1 = 0.5 + Z14_WIDTH / 2;
    double y1 = 0.5 + Z14_WIDTH / 2;
    double x2 = x1 + Z14_WIDTH;
    double y2 = y1 + Z14_WIDTH;
    double lat1 = GeoUtils.getWorldLat(y1);
    double lng1 = GeoUtils.getWorldLon(x1);
    double lat2 = GeoUtils.getWorldLat(y2);
    double lng2 = GeoUtils.getWorldLon(x2);

    var results = runWithReaderFeatures(
      Map.of("threads", "1"),
      List.of(
        newReaderFeature(newLineString(lng1, lat1, lng2, lat2), Map.of(
          "attr", "value"
        ))
      ),
      (in, features) -> (anyGeom ? features.anyGeometry("layer") : features.line("layer"))
        .setZoomRange(13, 14)
        .setBufferPixels(4)
    );

    assertSubmap(Map.of(
      TileCoord.ofXYZ(Z14_TILES / 2, Z14_TILES / 2, 14), List.of(
        feature(newLineString(128, 128, 260, 260), Map.of())
      ),
      TileCoord.ofXYZ(Z14_TILES / 2 + 1, Z14_TILES / 2 + 1, 14), List.of(
        feature(newLineString(-4, -4, 128, 128), Map.of())
      ),
      TileCoord.ofXYZ(Z13_TILES / 2, Z13_TILES / 2, 13), List.of(
        feature(newLineString(64, 64, 192, 192), Map.of())
      )
    ), results.tiles);
  }

  @Test
  void testPartialLine() throws Exception {
    double x1 = 0.5 + Z14_WIDTH / 2;
    double y1 = 0.5 + Z14_WIDTH / 2;
    double x2 = x1 + Z14_WIDTH;
    double y2 = y1 + Z14_WIDTH;
    double lat1 = GeoUtils.getWorldLat(y1);
    double lng1 = GeoUtils.getWorldLon(x1);
    double lat2 = GeoUtils.getWorldLat(y2);
    double lng2 = GeoUtils.getWorldLon(x2);

    var results = runWithReaderFeatures(
      Map.of("threads", "1"),
      List.of(
        newReaderFeature(newLineString(lng1, lat1, lng2, lat2), Map.of(
          "attr", "value"
        ))
      ),
      (in, features) -> features.partialLine("layer", 0, 0.5)
        .setZoomRange(13, 14)
        .setBufferPixels(4)
    );

    assertSubmap(Map.of(
      TileCoord.ofXYZ(Z14_TILES / 2, Z14_TILES / 2, 14), List.of(
        feature(newLineString(128, 128, 256, 256), Map.of())
      ),
      TileCoord.ofXYZ(Z14_TILES / 2 + 1, Z14_TILES / 2 + 1, 14), List.of(
        feature(newLineString(-4, -4, 0, 0), Map.of())
      ),
      TileCoord.ofXYZ(Z13_TILES / 2, Z13_TILES / 2, 13), List.of(
        feature(newLineString(64, 64, 128, 128), Map.of())
      )
    ), results.tiles);
  }

  @Test
  void testLineWithPartialAttr() throws Exception {
    double x1 = 0.5 + Z14_WIDTH / 2;
    double y1 = 0.5 + Z14_WIDTH / 2;
    double x2 = x1 + Z14_WIDTH;
    double y2 = y1 + Z14_WIDTH;
    double lat1 = GeoUtils.getWorldLat(y1);
    double lng1 = GeoUtils.getWorldLon(x1);
    double lat2 = GeoUtils.getWorldLat(y2);
    double lng2 = GeoUtils.getWorldLon(x2);

    var results = runWithReaderFeatures(
      Map.of("threads", "1"),
      List.of(
        newReaderFeature(newLineString(lng1, lat1, lng2, lat2), Map.of(
          "attr", "value"
        ))
      ),
      (in, features) -> features.line("layer")
        .linearRange(0, 0.25).setAttrWithMinzoom("k", "v", 14)
        .entireLine()
        .setZoomRange(13, 14)
        .setBufferPixels(4)
    );

    assertSubmap(Map.of(
      TileCoord.ofXYZ(Z14_TILES / 2, Z14_TILES / 2, 14), List.of(
        feature(newLineString(192, 192, 260, 260), Map.of()),
        feature(newLineString(128, 128, 192, 192), Map.of("k", "v"))
      ),
      TileCoord.ofXYZ(Z14_TILES / 2 + 1, Z14_TILES / 2 + 1, 14), List.of(
        feature(newLineString(-4, -4, 128, 128), Map.of())
      ),
      TileCoord.ofXYZ(Z13_TILES / 2, Z13_TILES / 2, 13), List.of(
        feature(newLineString(64, 64, 192, 192), Map.of())
      )
    ), results.tiles);
  }

  @Test
  void testLineStringDegenerateWhenUnscaled() throws Exception {
    double x1 = 0.5 + Z12_WIDTH / 2;
    double y1 = 0.5 + Z12_WIDTH / 2;
    double x2 = x1 + Z12_WIDTH / 4096 / 3;
    double y2 = y1 + Z12_WIDTH / 4096 / 3;
    double lat1 = GeoUtils.getWorldLat(y1);
    double lng1 = GeoUtils.getWorldLon(x1);
    double lat2 = GeoUtils.getWorldLat(y2);
    double lng2 = GeoUtils.getWorldLon(x2);

    var results = runWithReaderFeatures(
      Map.of("threads", "1"),
      List.of(
        newReaderFeature(newLineString(lng1, lat1, lng2, lat2), Map.of(
          "attr", "value"
        ))
      ),
      (in, features) -> features.line("layer")
        .setZoomRange(12, 12)
        .setMinPixelSize(0)
        .setBufferPixels(4)
    );

    assertSubmap(Map.of(), results.tiles);
  }

  @Test
  void testNumPointsAttr() throws Exception {
    double x1 = 0.5 + Z14_WIDTH / 2;
    double y1 = 0.5 + Z14_WIDTH / 2 - Z14_WIDTH / 2;
    double x2 = x1 + Z14_WIDTH;
    double y2 = y1 + Z14_WIDTH + Z14_WIDTH / 2;
    double x3 = x2 + Z14_WIDTH;
    double y3 = y2 + Z14_WIDTH;
    double lat1 = GeoUtils.getWorldLat(y1);
    double lng1 = GeoUtils.getWorldLon(x1);
    double lat2 = GeoUtils.getWorldLat(y2);
    double lng2 = GeoUtils.getWorldLon(x2);
    double lat3 = GeoUtils.getWorldLat(y3);
    double lng3 = GeoUtils.getWorldLon(x3);

    var results = runWithReaderFeatures(
      Map.of("threads", "1"),
      List.of(
        newReaderFeature(newLineString(lng1, lat1, lng2, lat2, lng3, lat3), Map.of(
          "attr", "value"
        ))
      ),
      (in, features) -> features.line("layer")
        .setZoomRange(13, 14)
        .setBufferPixels(4)
        .setNumPointsAttr("_numpoints")
    );

    assertSubmap(Map.of(
      TileCoord.ofXYZ(Z14_TILES / 2 + 2, Z14_TILES / 2 + 2, 14), List.of(
        feature(newLineString(-4, -4, 128, 128), Map.of(
          "_numpoints", 3L
        ))
      )
    ), results.tiles);
  }

  @Test
  void testMultiLineString() throws Exception {
    double x1 = 0.5 + Z14_WIDTH / 2;
    double y1 = 0.5 + Z14_WIDTH / 2;
    double x2 = x1 + Z14_WIDTH;
    double y2 = y1 + Z14_WIDTH;
    double lat1 = GeoUtils.getWorldLat(y1);
    double lng1 = GeoUtils.getWorldLon(x1);
    double lat2 = GeoUtils.getWorldLat(y2);
    double lng2 = GeoUtils.getWorldLon(x2);

    var results = runWithReaderFeatures(
      Map.of("threads", "1"),
      List.of(
        newReaderFeature(newMultiLineString(
          newLineString(lng1, lat1, lng2, lat2),
          newLineString(lng2, lat2, lng1, lat1)
        ), Map.of(
          "attr", "value"
        ))
      ),
      (in, features) -> features.line("layer")
        .setZoomRange(13, 14)
        .setBufferPixels(4)
    );

    assertSubmap(Map.of(
      TileCoord.ofXYZ(Z14_TILES / 2, Z14_TILES / 2, 14), List.of(
        feature(newMultiLineString(
          newLineString(128, 128, 260, 260),
          newLineString(260, 260, 128, 128)
        ), Map.of())
      ),
      TileCoord.ofXYZ(Z14_TILES / 2 + 1, Z14_TILES / 2 + 1, 14), List.of(
        feature(newMultiLineString(
          newLineString(-4, -4, 128, 128),
          newLineString(128, 128, -4, -4)
        ), Map.of())
      ),
      TileCoord.ofXYZ(Z13_TILES / 2, Z13_TILES / 2, 13), List.of(
        feature(newMultiLineString(
          newLineString(64, 64, 192, 192),
          newLineString(192, 192, 64, 64)
        ), Map.of())
      )
    ), results.tiles);
  }

  public List<Coordinate> z14CoordinateList(double... coords) {
    List<Coordinate> points = newCoordinateList(coords);
    points.forEach(c -> {
      c.x = GeoUtils.getWorldLon(0.5 + c.x * Z14_WIDTH);
      c.y = GeoUtils.getWorldLat(0.5 + c.y * Z14_WIDTH);
    });
    return points;
  }

  public List<Coordinate> z14PixelRectangle(double min, double max) {
    List<Coordinate> points = rectangleCoordList(min / 256d, max / 256d);
    points.forEach(c -> {
      c.x = GeoUtils.getWorldLon(0.5 + c.x * Z14_WIDTH);
      c.y = GeoUtils.getWorldLat(0.5 + c.y * Z14_WIDTH);
    });
    return points;
  }

  public List<Coordinate> z14CoordinatePixelList(double... coords) {
    return z14CoordinateList(DoubleStream.of(coords).map(c -> c / 256d).toArray());
  }

  public Point z14Point(double x, double y) {
    return newPoint(
      GeoUtils.getWorldLon(0.5 + x * Z14_WIDTH / 256),
      GeoUtils.getWorldLat(0.5 + y * Z14_WIDTH / 256)
    );
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void testPolygonWithHoleSpanningMultipleTiles(boolean anyGeom) throws Exception {
    List<Coordinate> outerPoints = z14CoordinateList(
      0.5, 0.5,
      3.5, 0.5,
      3.5, 2.5,
      0.5, 2.5,
      0.5, 0.5
    );
    List<Coordinate> innerPoints = z14CoordinateList(
      1.25, 1.25,
      1.75, 1.25,
      1.75, 1.75,
      1.25, 1.75,
      1.25, 1.25
    );

    var results = runWithReaderFeatures(
      Map.of("threads", "1"),
      List.of(
        newReaderFeature(newPolygon(
          outerPoints,
          List.of(innerPoints)
        ), Map.of())
      ),
      (in, features) -> (anyGeom ? features.anyGeometry("layer") : features.polygon("layer"))
        .setZoomRange(12, 14)
        .setBufferPixels(4)
    );

    assertEquals(Map.ofEntries(
      // Z12
      newTileEntry(Z12_TILES / 2, Z12_TILES / 2, 12, List.of(
        feature(newPolygon(
          rectangleCoordList(32, 32, 256 - 32, 128 + 32),
          List.of(
            rectangleCoordList(64 + 16, 128 - 16) // hole
          )
        ), Map.of())
      )),

      // Z13
      newTileEntry(Z13_TILES / 2, Z13_TILES / 2, 13, List.of(
        feature(newPolygon(
          rectangleCoordList(64, 256 + 4),
          List.of(rectangleCoordList(128 + 32, 256 - 32)) // hole
        ), Map.of())
      )),
      newTileEntry(Z13_TILES / 2 + 1, Z13_TILES / 2, 13, List.of(
        feature(rectangle(-4, 64, 256 - 64, 256 + 4), Map.of())
      )),
      newTileEntry(Z13_TILES / 2, Z13_TILES / 2 + 1, 13, List.of(
        feature(rectangle(64, -4, 256 + 4, 64), Map.of())
      )),
      newTileEntry(Z13_TILES / 2 + 1, Z13_TILES / 2 + 1, 13, List.of(
        feature(rectangle(-4, -4, 256 - 64, 64), Map.of())
      )),

      // Z14 - row 1
      newTileEntry(Z14_TILES / 2, Z14_TILES / 2, 14, List.of(
        feature(tileBottomRight(4), Map.of())
      )),
      newTileEntry(Z14_TILES / 2 + 1, Z14_TILES / 2, 14, List.of(
        feature(tileBottom(4), Map.of())
      )),
      newTileEntry(Z14_TILES / 2 + 2, Z14_TILES / 2, 14, List.of(
        feature(tileBottom(4), Map.of())
      )),
      newTileEntry(Z14_TILES / 2 + 3, Z14_TILES / 2, 14, List.of(
        feature(tileBottomLeft(4), Map.of())
      )),
      // Z14 - row 2
      newTileEntry(Z14_TILES / 2, Z14_TILES / 2 + 1, 14, List.of(
        feature(tileRight(4), Map.of())
      )),
      newTileEntry(Z14_TILES / 2 + 1, Z14_TILES / 2 + 1, 14, List.of(
        feature(newPolygon(
          tileFill(4),
          List.of(newCoordinateList(
            64, 64,
            192, 64,
            192, 192,
            64, 192,
            64, 64
          ))
        ), Map.of())
      )),
      newTileEntry(Z14_TILES / 2 + 2, Z14_TILES / 2 + 1, 14, List.of(
        feature(newPolygon(tileFill(5), List.of()), Map.of())
      )),
      newTileEntry(Z14_TILES / 2 + 3, Z14_TILES / 2 + 1, 14, List.of(
        feature(tileLeft(4), Map.of())
      )),
      // Z14 - row 3
      newTileEntry(Z14_TILES / 2, Z14_TILES / 2 + 2, 14, List.of(
        feature(tileTopRight(4), Map.of())
      )),
      newTileEntry(Z14_TILES / 2 + 1, Z14_TILES / 2 + 2, 14, List.of(
        feature(tileTop(4), Map.of())
      )),
      newTileEntry(Z14_TILES / 2 + 2, Z14_TILES / 2 + 2, 14, List.of(
        feature(tileTop(4), Map.of())
      )),
      newTileEntry(Z14_TILES / 2 + 3, Z14_TILES / 2 + 2, 14, List.of(
        feature(tileTopLeft(4), Map.of())
      ))
    ), results.tiles);
  }

  @Test
  void testZ15Fill() throws Exception {
    List<Coordinate> outerPoints = z14CoordinateList(
      -2, -2,
      2, -2,
      2, 2,
      -2, 2,
      -2, -2
    );

    var results = runWithReaderFeatures(
      Map.of("threads", "1", "maxzoom", "15"),
      List.of(
        newReaderFeature(newPolygon(
          outerPoints
        ), Map.of())
      ),
      (in, features) -> features.polygon("layer")
        .setZoomRange(15, 15)
        .setBufferPixels(4)
    );

    assertEquals(List.of(
      feature(newPolygon(tileFill(5)), Map.of())
    ), results.tiles.get(TileCoord.ofXYZ(Z15_TILES / 2, Z15_TILES / 2, 15)));
  }

  @Test
  void testFullWorldPolygon() throws Exception {
    var results = runWithReaderFeatures(
      Map.of("threads", "1"),
      List.of(
        newReaderFeature(WORLD_POLYGON, Map.of())
      ),
      (in, features) -> features.polygon("layer")
        .setZoomRange(0, 6)
        .setBufferPixels(4)
    );

    assertEquals(5461, results.tiles.size());
    // spot-check one filled tile
    assertEquals(List.of(rectangle(-5, 256 + 5).norm()), results.tiles.get(TileCoord.ofXYZ(
      Z4_TILES / 2, Z4_TILES / 2, 4
    )).stream().map(d -> d.geometry().geom().norm()).toList());
  }

  @Test
  void testSkipFill() throws Exception {
    var results = runWithReaderFeatures(
      Map.of("threads", "1", "skip-filled-tiles", "true"),
      List.of(
        newReaderFeature(WORLD_POLYGON, Map.of())
      ),
      (in, features) -> features.polygon("layer")
        .setZoomRange(0, 6)
        .setBufferPixels(4)
    );

    assertEquals(481, results.tiles.size());
    // spot-check one filled tile does not exist
    assertNull(results.tiles.get(TileCoord.ofXYZ(
      Z4_TILES / 2, Z4_TILES / 2, 4
    )));
  }

  @ParameterizedTest
  @CsvSource({
    "chesapeake.wkb, 4076",
    "mdshore.wkb,    19904",
    "njshore.wkb,    10571",
    "kobroor.wkb,    21693"
  })
  void testComplexShorelinePolygons__TAKES_A_MINUTE_OR_TWO(String fileName, int expected)
    throws Exception {
    LOGGER.warn("Testing complex shoreline processing for " + fileName + " ...");
    Geometry geometry = new WKBReader()
      .read(new InputStreamInStream(Files.newInputStream(TestUtils.pathToResource(fileName))));
    assertNotNull(geometry);

    // automatically checks for self-intersections
    var results = runWithReaderFeatures(
      Map.of("threads", "1"),
      List.of(
        newReaderFeature(geometry, Map.of())
      ),
      (in, features) -> features.polygon("layer")
        .setZoomRange(0, 14)
        .setBufferPixels(4)
    );

    assertEquals(expected, results.tiles.size());
  }

  @Test
  void testReorderNestedMultipolygons() throws Exception {
    List<Coordinate> outerPoints1 = worldRectangle(10d / 256, 240d / 256);
    List<Coordinate> innerPoints1 = worldRectangle(20d / 256, 230d / 256);
    List<Coordinate> outerPoints2 = worldRectangle(30d / 256, 220d / 256);
    List<Coordinate> innerPoints2 = worldRectangle(40d / 256, 210d / 256);

    var results = runWithReaderFeatures(
      Map.of("threads", "1"),
      List.of(
        newReaderFeature(newMultiPolygon(
          newPolygon(outerPoints2, List.of(innerPoints2)),
          newPolygon(outerPoints1, List.of(innerPoints1))
        ), Map.of())
      ),
      (in, features) -> features.polygon("layer")
        .setZoomRange(0, 0)
        .setBufferPixels(0)
    );

    var tileContents = results.tiles.get(TileCoord.ofXYZ(0, 0, 0));
    assertEquals(1, tileContents.size());
    Geometry geom = tileContents.getFirst().geometry().geom();
    assertTrue(geom instanceof MultiPolygon, geom.toString());
    MultiPolygon multiPolygon = (MultiPolygon) geom;
    assertSameNormalizedFeature(newPolygon(
      rectangleCoordList(10, 240),
      List.of(rectangleCoordList(20, 230))
    ), multiPolygon.getGeometryN(0));
    assertSameNormalizedFeature(newPolygon(
      rectangleCoordList(30, 220),
      List.of(rectangleCoordList(40, 210))
    ), multiPolygon.getGeometryN(1));
    assertEquals(2, multiPolygon.getNumGeometries());
  }

  @Test
  void testOsmPoint() throws Exception {
    var results = runWithOsmElements(
      Map.of("threads", "1"),
      List.of(
        with(new OsmElement.Node(1, 0, 0), t -> t.setTag("attr", "value"))
      ),
      (in, features) -> {
        if (in.isPoint()) {
          features.point("layer")
            .setZoomRange(0, 0)
            .setAttr("name", "name value")
            .inheritAttrFromSource("attr");
        }
      }
    );

    assertSubmap(Map.of(
      TileCoord.ofXYZ(0, 0, 0), List.of(
        feature(newPoint(128, 128), Map.of(
          "attr", "value",
          "name", "name value"
        ), 11)
      )
    ), results.tiles);
  }

  @Test
  void testOsmPointSkipPass1() throws Exception {
    var results = run(
      Map.of("threads", "1"),
      (featureGroup, profile, config) -> {
        List<? extends OsmElement> osmElements = List.<OsmElement>of(
          with(new OsmElement.Node(1, 0, 0), t -> t.setTag("attr", "value"))
        );
        OsmBlockSource elems = next -> {
          // process the same order they come in from an OSM file
          next.accept(
            OsmBlockSource.Block.of(osmElements.stream().filter(e -> e instanceof OsmElement.Other).toList()));
          next.accept(OsmBlockSource.Block.of(osmElements.stream().filter(e -> e instanceof OsmElement.Node).toList()));
          next.accept(OsmBlockSource.Block.of(osmElements.stream().filter(e -> e instanceof OsmElement.Way).toList()));
          next.accept(
            OsmBlockSource.Block.of(osmElements.stream().filter(e -> e instanceof OsmElement.Relation).toList()));
        };
        var nodeMap = LongLongMap.newInMemorySortedTable();
        var multipolygons = LongLongMultimap.newInMemoryReplaceableMultimap();
        try (var reader = new OsmReader("osm", () -> elems, nodeMap, multipolygons, profile, Stats.inMemory())) {
          // skip pass 1
          reader.pass2(featureGroup, config);
        }
      },
      TestProfile.processSourceFeatures((in, features) -> {
        if (in.isPoint()) {
          features.point("layer")
            .setZoomRange(0, 0)
            .setAttr("name", "name value")
            .inheritAttrFromSource("attr");
        }
      })
    );

    assertSubmap(Map.of(
      TileCoord.ofXYZ(0, 0, 0), List.of(
        feature(newPoint(128, 128), Map.of(
          "attr", "value",
          "name", "name value"
        ))
      )
    ), results.tiles);
  }

  @Test
  void testExceptionWhileProcessingOsm() {
    assertThrows(RuntimeException.class, () -> runWithOsmElements(
      Map.of("threads", "1"),
      List.of(
        with(new OsmElement.Node(1, 0, 0), t -> t.setTag("attr", "value"))
      ),
      (in, features) -> {
        throw new ExpectedError();
      }
    ));
  }

  @Test
  void testOsmLine() throws Exception {
    var results = runWithOsmElements(
      Map.of("threads", "1"),
      List.of(
        new OsmElement.Node(1, 0, 0),
        new OsmElement.Node(2, GeoUtils.getWorldLat(0.75), GeoUtils.getWorldLon(0.75)),
        with(new OsmElement.Way(3), way -> {
          way.setTag("attr", "value");
          way.nodes().add(1, 2);
        })
      ),
      (in, features) -> {
        if (in.canBeLine()) {
          features.line("layer")
            .setZoomRange(0, 0)
            .setAttr("name", "name value")
            .inheritAttrFromSource("attr");
        }
      }
    );

    assertSubmap(Map.of(
      TileCoord.ofXYZ(0, 0, 0), List.of(
        feature(newLineString(128, 128, 192, 192), Map.of(
          "attr", "value",
          "name", "name value"
        ), 32)
      )
    ), results.tiles);
  }

  @Test
  void testOsmLineOrPolygon() throws Exception {
    var results = runWithOsmElements(
      Map.of("threads", "1"),
      List.of(
        new OsmElement.Node(1, GeoUtils.getWorldLat(0.25), GeoUtils.getWorldLon(0.25)),
        new OsmElement.Node(2, GeoUtils.getWorldLat(0.25), GeoUtils.getWorldLon(0.75)),
        new OsmElement.Node(3, GeoUtils.getWorldLat(0.75), GeoUtils.getWorldLon(0.75)),
        new OsmElement.Node(4, GeoUtils.getWorldLat(0.75), GeoUtils.getWorldLon(0.25)),
        with(new OsmElement.Way(6), way -> {
          way.setTag("attr", "value");
          way.nodes().add(1, 2, 3, 4, 1);
        })
      ),
      (in, features) -> {
        if (in.canBeLine()) {
          features.line("layer")
            .setZoomRange(0, 0)
            .setAttr("name", "name value1")
            .inheritAttrFromSource("attr");
        }
        if (in.canBePolygon()) {
          features.polygon("layer")
            .setZoomRange(0, 0)
            .setAttr("name", "name value2")
            .inheritAttrFromSource("attr");
        }
      }
    );

    assertSubmap(sortListValues(Map.of(
      TileCoord.ofXYZ(0, 0, 0), List.of(
        feature(newLineString(
          64, 64,
          192, 64,
          192, 192,
          64, 192,
          64, 64
        ), Map.of(
          "attr", "value",
          "name", "name value1"
        )),
        feature(rectangle(64, 192), Map.of(
          "attr", "value",
          "name", "name value2"
        ))
      )
    )), sortListValues(results.tiles));
  }

  @ParameterizedTest
  @ValueSource(strings = {"multipolygon", "boundary", "land_area"})
  void testOsmMultipolygon(String relationType) throws Exception {
    record TestRelationInfo(long id, String name) implements OsmRelationInfo {}
    var results = runWithOsmElements(
      Map.of("threads", "1"),
      List.of(
        new OsmElement.Node(1, GeoUtils.getWorldLat(0.125), GeoUtils.getWorldLon(0.125)),
        new OsmElement.Node(2, GeoUtils.getWorldLat(0.125), GeoUtils.getWorldLon(0.875)),
        new OsmElement.Node(3, GeoUtils.getWorldLat(0.875), GeoUtils.getWorldLon(0.875)),
        new OsmElement.Node(4, GeoUtils.getWorldLat(0.875), GeoUtils.getWorldLon(0.125)),

        new OsmElement.Node(5, GeoUtils.getWorldLat(0.25), GeoUtils.getWorldLon(0.25)),
        new OsmElement.Node(6, GeoUtils.getWorldLat(0.25), GeoUtils.getWorldLon(0.75)),
        new OsmElement.Node(7, GeoUtils.getWorldLat(0.75), GeoUtils.getWorldLon(0.75)),
        new OsmElement.Node(8, GeoUtils.getWorldLat(0.75), GeoUtils.getWorldLon(0.25)),

        new OsmElement.Node(9, GeoUtils.getWorldLat(0.375), GeoUtils.getWorldLon(0.375)),
        new OsmElement.Node(10, GeoUtils.getWorldLat(0.375), GeoUtils.getWorldLon(0.625)),
        new OsmElement.Node(11, GeoUtils.getWorldLat(0.625), GeoUtils.getWorldLon(0.625)),
        new OsmElement.Node(12, GeoUtils.getWorldLat(0.625), GeoUtils.getWorldLon(0.375)),
        new OsmElement.Node(13, GeoUtils.getWorldLat(0.375 + 1e-12), GeoUtils.getWorldLon(0.375)),

        with(new OsmElement.Way(14), way -> way.nodes().add(1, 2, 3, 4, 1)),
        with(new OsmElement.Way(15), way -> way.nodes().add(5, 6, 7, 8, 5)),
        with(new OsmElement.Way(16), way -> way.nodes().add(9, 10, 11, 12, 13)),

        with(new OsmElement.Relation(17), rel -> {
          rel.setTag("type", relationType);
          rel.setTag("attr", "value");
          rel.setTag("should_emit", "yes");
          rel.members().add(new OsmElement.Relation.Member(OsmElement.Type.WAY, 14, "outer"));
          rel.members().add(new OsmElement.Relation.Member(OsmElement.Type.WAY, 15, null)); // missing
          rel.members().add(new OsmElement.Relation.Member(OsmElement.Type.WAY, 16, "inner")); // incorrect
          rel.members().add(new OsmElement.Relation.Member(OsmElement.Type.WAY, 14, "outer")); // duplicate
        }),
        with(new OsmElement.Relation(18), rel -> {
          rel.setTag("type", "relation");
          rel.setTag("name", "rel name");
          rel.members().add(new OsmElement.Relation.Member(OsmElement.Type.WAY, 17, "outer"));
        })
      ),
      in -> in.hasTag("type", "relation") ?
        List.of(new TestRelationInfo(in.id(), in.getString("name"))) :
        null,
      (in, features) -> {
        if (in.hasTag("should_emit")) {
          features.polygon("layer")
            .setZoomRange(0, 0)
            .setAttr("name", "name value")
            .inheritAttrFromSource("attr")
            .setAttr("relname",
              in.relationInfo(TestRelationInfo.class).stream().map(c -> c.relation().name).findFirst().orElse(null));
        }
      }
    );

    assertSubmap(Map.of(
      TileCoord.ofXYZ(0, 0, 0), List.of(
        feature(newMultiPolygon(
          newPolygon(
            rectangleCoordList(0.125 * 256, 0.875 * 256),
            List.of(
              rectangleCoordList(0.25 * 256, 0.75 * 256)
            )
          ),
          rectangle(0.375 * 256, 0.625 * 256)
        ), Map.of(
          "attr", "value",
          "name", "name value",
          "relname", "rel name"
        ), 173)
      )
    ), results.tiles);
  }

  @Test
  void testOsmLineInRelation() throws Exception {
    record TestRelationInfo(long id, String name) implements OsmRelationInfo {}
    var results = runWithOsmElements(
      Map.of("threads", "1"),
      List.of(
        new OsmElement.Node(1, 0, 0),
        new OsmElement.Node(2, GeoUtils.getWorldLat(0.375), 0),
        new OsmElement.Node(3, GeoUtils.getWorldLat(0.25), 0),
        new OsmElement.Node(4, GeoUtils.getWorldLat(0.125), 0),
        with(new OsmElement.Way(5), way -> {
          way.setTag("attr", "value1");
          way.nodes().add(1, 2);
        }),
        with(new OsmElement.Way(6), way -> {
          way.setTag("attr", "value2");
          way.nodes().add(3, 4);
        }),
        with(new OsmElement.Relation(6), rel -> {
          rel.setTag("name", "relation name");
          rel.members().add(new OsmElement.Relation.Member(OsmElement.Type.WAY, 6, "role"));
        })
      ),
      (relation) -> {
        if (relation.hasTag("name", "relation name")) {
          return List.of(new TestRelationInfo(relation.id(), relation.getString("name")));
        }
        return null;
      }, (in, features) -> {
        var relationInfos = in.relationInfo(TestRelationInfo.class);
        var firstRelation = relationInfos.stream().findFirst();
        if (in.canBeLine()) {
          features.line("layer")
            .setZoomRange(0, 0)
            .setAttr("relname", firstRelation.map(d -> d.relation().name).orElse(null))
            .inheritAttrFromSource("attr")
            .setAttr("relrole", firstRelation.map(OsmReader.RelationMember::role).orElse(null));
        }
      }
    );

    assertSubmap(sortListValues(Map.of(
      TileCoord.ofXYZ(0, 0, 0), List.of(
        feature(newLineString(128, 128, 128, 0.375 * 256), Map.of(
          "attr", "value1"
        )),
        feature(newLineString(128, 0.25 * 256, 128, 0.125 * 256), Map.of(
          "attr", "value2",
          "relname", "relation name",
          "relrole", "role"
        ))
      )
    )), sortListValues(results.tiles));
  }

  @Test
  void testPreprocessOsmNodesAndWays() throws Exception {
    HashMap<Long, Long> nodes1 = new HashMap<>();
    Set<Long> nodes2 = new HashSet<>();
    var profile = new Profile.NullProfile() {
      @Override
      public void preprocessOsmNode(OsmElement.Node node) {
        if (node.hasTag("a", "b")) {
          nodes1.put(node.id(), node.id());
        }
      }

      @Override
      public void preprocessOsmWay(OsmElement.Way way) {
        Long featureId = nodes1.get(way.nodes().get(0));
        if (featureId != null) {
          nodes2.add(featureId);
        }
      }

      @Override
      public void processFeature(SourceFeature sourceFeature, FeatureCollector features) {
        if (sourceFeature.isPoint() && nodes2.contains(sourceFeature.id())) {
          features.point("start_nodes")
            .setMaxZoom(0);
        }
      }
    };
    var results = run(
      Map.of("threads", "1"),
      (featureGroup, p, config) -> processOsmFeatures(featureGroup, p, config, List.of(
        with(new OsmElement.Node(1, 0, 0), node -> node.setTag("a", "b")),
        new OsmElement.Node(2, GeoUtils.getWorldLat(0.375), 0),
        with(new OsmElement.Way(3), way -> {
          way.nodes().add(1, 2);
        }),
        with(new OsmElement.Way(4), way -> {
          way.nodes().add(1, 2);
        })
      )),
      profile
    );

    assertSubmap(sortListValues(Map.of(
      TileCoord.ofXYZ(0, 0, 0), List.of(
        feature(newPoint(128, 128), Map.of())
      )
    )), sortListValues(results.tiles));
  }

  @Test
  void testPostProcessNodeUseLabelGridRank() throws Exception {
    double y = 0.5 + Z14_WIDTH / 2;
    double lat = GeoUtils.getWorldLat(y);

    double x1 = 0.5 + Z14_WIDTH / 4;
    double lng1 = GeoUtils.getWorldLon(x1);
    double lng2 = GeoUtils.getWorldLon(x1 + Z14_WIDTH * 10d / 256);
    double lng3 = GeoUtils.getWorldLon(x1 + Z14_WIDTH * 20d / 256);

    var results = runWithReaderFeatures(
      Map.of("threads", "1"),
      List.of(
        newReaderFeature(newPoint(lng1, lat), Map.of("rank", "1")),
        newReaderFeature(newPoint(lng2, lat), Map.of("rank", "2")),
        newReaderFeature(newPoint(lng3, lat), Map.of("rank", "3"))
      ),
      (in, features) -> features.point("layer")
        .setZoomRange(13, 14)
        .inheritAttrFromSource("rank")
        .setSortKey(Integer.parseInt(in.getTag("rank").toString()))
        .setPointLabelGridPixelSize(13, 8)
        .setBufferPixels(8),
      (layer, zoom, items) -> {
        if ("layer".equals(layer) && zoom == 13) {
          List<VectorTile.Feature> result = new ArrayList<>(items.size());
          Map<Long, Integer> rankInGroup = new HashMap<>();
          for (var item : items) {
            result.add(item.copyWithExtraAttrs(Map.of(
              "grouprank", rankInGroup.merge(item.group(), 1, Integer::sum)
            )));
          }
          return result;
        } else {
          return items;
        }
      }
    );

    assertSubmap(Map.of(
      TileCoord.ofXYZ(Z14_TILES / 2, Z14_TILES / 2, 14), List.of(
        feature(newPoint(64, 128), Map.of("rank", "1")),
        feature(newPoint(74, 128), Map.of("rank", "2")),
        feature(newPoint(84, 128), Map.of("rank", "3"))
      ),
      TileCoord.ofXYZ(Z13_TILES / 2, Z13_TILES / 2, 13), List.of(
        feature(newPoint(32, 64), Map.of("rank", "1", "grouprank", 1L)),
        feature(newPoint(37, 64), Map.of("rank", "2", "grouprank", 2L)),
        // separate group
        feature(newPoint(42, 64), Map.of("rank", "3", "grouprank", 1L))
      )
    ), results.tiles);
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void testMergeLineStrings(boolean connectEndpoints) throws Exception {
    double y = 0.5 + Z15_WIDTH / 2;
    double lat = GeoUtils.getWorldLat(y);

    double x1 = 0.5 + Z15_WIDTH / 4;
    double lng1 = GeoUtils.getWorldLon(x1);
    double lng2 = GeoUtils.getWorldLon(x1 + Z15_WIDTH * 10d / 256);
    double lng3 = GeoUtils.getWorldLon(x1 + Z15_WIDTH * 20d / 256);
    double lng4 = GeoUtils.getWorldLon(x1 + Z15_WIDTH * 30d / 256);

    var results = runWithReaderFeatures(
      Map.of("threads", "1", "maxzoom", "15"),
      List.of(
        // merge at z13 (same "group"):
        newReaderFeature(newLineString(
          lng1, lat,
          lng2, lat
        ), Map.of("group", "1", "other", "1")),
        newReaderFeature(newLineString(
          lng2, lat,
          lng3, lat
        ), Map.of("group", "1", "other", "2")),
        // don't merge at z13:
        newReaderFeature(newLineString(
          lng3, lat,
          lng4, lat
        ), Map.of("group", "2", "other", "3"))
      ),
      (in, features) -> features.line("layer")
        .setMinZoom(13)
        .setAttrWithMinzoom("z14attr", in.getTag("other"), 14)
        .inheritAttrFromSource("group"),
      (layer, zoom, items) -> connectEndpoints ?
        FeatureMerge.mergeLineStrings(items, 0, 0, 0) :
        FeatureMerge.mergeMultiLineString(items)
    );

    assertSubmap(sortListValues(Map.of(
      TileCoord.ofXYZ(Z15_TILES / 2, Z15_TILES / 2, 15), List.of(
        feature(newLineString(64, 128, 74, 128), Map.of("group", "1", "z14attr", "1")),
        feature(newLineString(74, 128, 84, 128), Map.of("group", "1", "z14attr", "2")),
        feature(newLineString(84, 128, 94, 128), Map.of("group", "2", "z14attr", "3"))
      ),
      TileCoord.ofXYZ(Z14_TILES / 2, Z14_TILES / 2, 14), List.of(
        feature(newLineString(32, 64, 37, 64), Map.of("group", "1", "z14attr", "1")),
        feature(newLineString(37, 64, 42, 64), Map.of("group", "1", "z14attr", "2")),
        feature(newLineString(42, 64, 47, 64), Map.of("group", "2", "z14attr", "3"))
      ),
      TileCoord.ofXYZ(Z13_TILES / 2, Z13_TILES / 2, 13), connectEndpoints ? List.of(
        // merge 32->37 and 37->42 since they have same attrs
        feature(newLineString(16, 32, 21, 32), Map.of("group", "1")),
        feature(newLineString(21, 32, 23.5, 32), Map.of("group", "2"))
      ) : List.of(
        feature(newMultiLineString(
          newLineString(16, 32, 18.5, 32),
          newLineString(18.5, 32, 21, 32)
        ), Map.of("group", "1")),
        feature(newLineString(21, 32, 23.5, 32), Map.of("group", "2"))
      )
    )), sortListValues(results.tiles));
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void postProcessTileFeatures(boolean postProcessLayersToo) throws Exception {
    double y = 0.5 + Z15_WIDTH / 2;
    double lat = GeoUtils.getWorldLat(y);

    double x1 = 0.5 + Z15_WIDTH / 4;
    double lng1 = GeoUtils.getWorldLon(x1);
    double lng2 = GeoUtils.getWorldLon(x1 + Z15_WIDTH * 10d / 256);

    List<SimpleFeature> features1 = List.of(
      newReaderFeature(newPoint(lng1, lat), Map.of("from", "a")),
      newReaderFeature(newPoint(lng2, lat), Map.of("from", "b"))
    );
    var testProfile = new Profile.NullProfile() {
      @Override
      public void processFeature(SourceFeature sourceFeature, FeatureCollector features) {
        features.point(sourceFeature.getString("from"))
          .inheritAttrFromSource("from")
          .setMinZoom(15);
      }

      @Override
      public Map<String, List<VectorTile.Feature>> postProcessTileFeatures(TileCoord tileCoord,
        Map<String, List<VectorTile.Feature>> layers) {
        List<VectorTile.Feature> features = new ArrayList<>();
        features.addAll(layers.get("a"));
        features.addAll(layers.get("b"));
        return Map.of("c", features);
      }

      @Override
      public List<VectorTile.Feature> postProcessLayerFeatures(String layer, int zoom, List<VectorTile.Feature> items) {
        return postProcessLayersToo ? items.reversed() : items;
      }
    };
    var results = run(
      Map.of("threads", "1", "maxzoom", "15"),
      (featureGroup, profile, config) -> processReaderFeatures(featureGroup, profile, config, features1),
      testProfile);

    assertSubmap(sortListValues(Map.of(
      TileCoord.ofXYZ(Z15_TILES / 2, Z15_TILES / 2, 15), List.of(
        feature(newPoint(64, 128), "c", Map.of("from", "a")),
        feature(newPoint(74, 128), "c", Map.of("from", "b"))
      )
    )), sortListValues(results.tiles));
  }

  @Test
  void testMergeLineStringsIgnoresRoundingIntersections() throws Exception {
    double y = 0.5 + Z14_WIDTH / 2;
    double lat = GeoUtils.getWorldLat(y);
    double lat2a = GeoUtils.getWorldLat(y + Z14_WIDTH * 0.5 / 4096);
    double lat2b = GeoUtils.getWorldLat(y + Z14_WIDTH * 1.5 / 4096);
    double lat3 = GeoUtils.getWorldLat(y + Z14_WIDTH * 10 / 4096);

    double x1 = 0.5 + Z14_WIDTH / 4;
    double lng1 = GeoUtils.getWorldLon(x1);
    double lng2 = GeoUtils.getWorldLon(x1 + Z14_WIDTH * 10d / 256);
    double lng3 = GeoUtils.getWorldLon(x1 + Z14_WIDTH * 20d / 256);

    var results = runWithReaderFeatures(
      Map.of("threads", "1"),
      List.of(
        // group two parallel lines that almost touch at the midpoint
        // need to retain extra precision while merging to ensure it
        // doesn't confuse the line merger
        newReaderFeature(newLineString(
          lng1, lat,
          lng2, lat2a
        ), Map.of()),
        newReaderFeature(newLineString(
          lng2, lat2a,
          lng3, lat
        ), Map.of()),

        newReaderFeature(newLineString(
          lng1, lat3,
          lng2, lat2b
        ), Map.of()),
        newReaderFeature(newLineString(
          lng2, lat2b,
          lng3, lat3
        ), Map.of())
      ),
      (in, features) -> features.line("layer").setZoomRange(13, 13),
      (layer, zoom, items) -> FeatureMerge.mergeLineStrings(items, 0, 0, 0)
    );

    assertSubmap(sortListValues(Map.of(
      TileCoord.ofXYZ(Z13_TILES / 2, Z13_TILES / 2, 13), List.of(
        feature(newMultiLineString(
          newLineString(32, 64.3125, 37, 64.0625, 42, 64.3125),
          newLineString(32, 64, 37, 64.0625, 42, 64)
        ), Map.of())
      )
    )), sortListValues(results.tiles));
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void testMergePolygons(boolean unionOverlapping) throws Exception {
    var results = runWithReaderFeatures(
      Map.of("threads", "1"),
      List.of(
        // merge same group:
        newReaderFeature(newPolygon(z14CoordinatePixelList(
          10, 10,
          20, 10,
          20, 20,
          10, 20,
          10, 10
        )), Map.of("group", "1")),
        newReaderFeature(newPolygon(z14CoordinatePixelList(
          20.5, 10,
          30, 10,
          30, 20,
          20.5, 20,
          20.5, 10
        )), Map.of("group", "1")),
        // don't merge - different group:
        newReaderFeature(newPolygon(z14CoordinatePixelList(
          10, 20.5,
          20, 20.5,
          20, 30,
          10, 30,
          10, 20.5
        )), Map.of("group", "2"))
      ),
      (in, features) -> features.polygon("layer")
        .setZoomRange(14, 14)
        .inheritAttrFromSource("group"),
      (layer, zoom, items) -> unionOverlapping ? FeatureMerge.mergeNearbyPolygons(
        items,
        0,
        0,
        1,
        1
      ) : FeatureMerge.mergeMultiPolygon(items)
    );

    if (unionOverlapping) {
      assertSubmap(sortListValues(Map.of(
        TileCoord.ofXYZ(Z14_TILES / 2, Z14_TILES / 2, 14), List.of(
          feature(rectangle(10, 10, 30, 20), Map.of("group", "1")),
          feature(rectangle(10, 20.5, 20, 30), Map.of("group", "2"))
        )
      )), sortListValues(results.tiles));
    } else {
      assertSubmap(sortListValues(Map.of(
        TileCoord.ofXYZ(Z14_TILES / 2, Z14_TILES / 2, 14), List.of(
          feature(
            newMultiPolygon(
              rectangle(10, 10, 20, 20),
              rectangle(20.5, 10, 30, 20)
            ), Map.of("group", "1")),
          feature(rectangle(10, 20.5, 20, 30), Map.of("group", "2"))
        )
      )), sortListValues(results.tiles));
    }
  }

  @Test
  void testCombineMultiPoint() throws Exception {
    var results = runWithReaderFeatures(
      Map.of("threads", "1"),
      List.of(
        // merge same group:
        newReaderFeature(z14Point(0, 0), Map.of("group", "1")),
        newReaderFeature(newMultiPoint(
          z14Point(1, 1),
          z14Point(2, 2)
        ), Map.of("group", "1")),
        // don't merge - different group:
        newReaderFeature(z14Point(3, 3), Map.of("group", "2"))
      ),
      (in, features) -> features.point("layer")
        .setZoomRange(14, 14)
        .setBufferPixels(0)
        .inheritAttrFromSource("group"),
      (layer, zoom, items) -> FeatureMerge.mergeMultiPoint(items)
    );

    assertSubmap(sortListValues(Map.of(
      TileCoord.ofXYZ(Z14_TILES / 2, Z14_TILES / 2, 14), List.of(
        feature(newMultiPoint(
          newPoint(0, 0),
          newPoint(1, 1),
          newPoint(2, 2)
        ), Map.of("group", "1")),
        feature(newPoint(3, 3), Map.of("group", "2"))
      )
    )), sortListValues(results.tiles));
  }

  @Test
  void testReduceMaxPointBuffer() throws Exception {
    var results = runWithReaderFeatures(
      Map.of(
        "threads", "1",
        "max-point-buffer", "1"
      ),
      List.of(
        newReaderFeature(z14Point(0, 0), Map.of("group", "1")),
        newReaderFeature(newMultiPoint(
          z14Point(-1, -1),
          z14Point(-2, -2) // should get filtered out
        ), Map.of("group", "1")),
        // don't merge - different group:
        newReaderFeature(z14Point(257, 257), Map.of("group", "2")),
        newReaderFeature(z14Point(258, 258), Map.of("group", "3")) // filter out
      ),
      (in, features) -> features.point("layer")
        .setZoomRange(14, 14)
        .setBufferPixels(10)
        .inheritAttrFromSource("group")
    );

    assertSubmap(sortListValues(Map.of(
      TileCoord.ofXYZ(Z14_TILES / 2, Z14_TILES / 2, 14), List.of(
        feature(newPoint(-1, -1), Map.of("group", "1")),
        feature(newPoint(0, 0), Map.of("group", "1")),
        feature(newPoint(257, 257), Map.of("group", "2"))
      )
    )), sortListValues(results.tiles));
  }

  @Test
  void testReaderProfileFinish() throws Exception {
    double y = 0.5 + Z14_WIDTH / 2;
    double lat = GeoUtils.getWorldLat(y);

    double x1 = 0.5 + Z14_WIDTH / 4;
    double lng1 = GeoUtils.getWorldLon(x1);
    double lng2 = GeoUtils.getWorldLon(x1 + Z14_WIDTH * 10d / 256);

    var results = runWithReaderFeaturesProfile(
      Map.of("threads", "1"),
      List.of(
        newReaderFeature(newPoint(lng1, lat), Map.of("a", 1, "b", 2)),
        newReaderFeature(newPoint(lng2, lat), Map.of("a", 3, "b", 4))
      ),
      new Profile.NullProfile() {
        private final List<SourceFeature> featureList = new CopyOnWriteArrayList<>();

        @Override
        public void processFeature(SourceFeature in, FeatureCollector features) {
          featureList.add(in);
        }

        @Override
        public void finish(String name, FeatureCollector.Factory featureCollectors,
          Consumer<FeatureCollector.Feature> next) {
          if ("test".equals(name)) {
            for (SourceFeature in : featureList) {
              var features = featureCollectors.get(in);
              features.point("layer")
                .setZoomRange(13, 14)
                .inheritAttrFromSource("a");
              for (var feature : features) {
                next.accept(feature);
              }
            }
          }
        }
      }
    );

    assertSubmap(sortListValues(Map.of(
      TileCoord.ofXYZ(Z14_TILES / 2, Z14_TILES / 2, 14), List.of(
        feature(newPoint(64, 128), Map.of("a", 1L)),
        feature(newPoint(74, 128), Map.of("a", 3L))
      ),
      TileCoord.ofXYZ(Z13_TILES / 2, Z13_TILES / 2, 13), List.of(
        // merge 32->37 and 37->42 since they have same attrs
        feature(newPoint(32, 64), Map.of("a", 1L)),
        feature(newPoint(37, 64), Map.of("a", 3L))
      )
    )), sortListValues(results.tiles));
  }

  @Test
  void testOsmProfileFinish() throws Exception {
    double y = 0.5 + Z14_WIDTH / 2;
    double lat = GeoUtils.getWorldLat(y);

    double x1 = 0.5 + Z14_WIDTH / 4;
    double lng1 = GeoUtils.getWorldLon(x1);
    double lng2 = GeoUtils.getWorldLon(x1 + Z14_WIDTH * 10d / 256);

    var results = runWithOsmElements(
      Map.of("threads", "1"),
      List.of(
        with(new OsmElement.Node(1, lat, lng1), t -> t.setTag("a", 1)),
        with(new OsmElement.Node(2, lat, lng2), t -> t.setTag("a", 3))
      ),
      new Profile.NullProfile() {
        private final List<SourceFeature> featureList = new CopyOnWriteArrayList<>();

        @Override
        public void processFeature(SourceFeature in, FeatureCollector features) {
          featureList.add(in);
        }

        @Override
        public void finish(String name, FeatureCollector.Factory featureCollectors,
          Consumer<FeatureCollector.Feature> next) {
          if ("osm".equals(name)) {
            for (SourceFeature in : featureList) {
              var features = featureCollectors.get(in);
              features.point("layer")
                .setZoomRange(13, 14)
                .inheritAttrFromSource("a");
              for (var feature : features) {
                next.accept(feature);
              }
            }
          }
        }
      }
    );

    assertSubmap(sortListValues(Map.of(
      TileCoord.ofXYZ(Z14_TILES / 2, Z14_TILES / 2, 14), List.of(
        feature(newPoint(64, 128), Map.of("a", 1L)),
        feature(newPoint(74, 128), Map.of("a", 3L))
      ),
      TileCoord.ofXYZ(Z13_TILES / 2, Z13_TILES / 2, 13), List.of(
        // merge 32->37 and 37->42 since they have same attrs
        feature(newPoint(32, 64), Map.of("a", 1L)),
        feature(newPoint(37, 64), Map.of("a", 3L))
      )
    )), sortListValues(results.tiles));
  }

  @Test
  void testOsmProfileFinishForwardingProfile() throws Exception {
    double y = 0.5 + Z14_WIDTH / 2;
    double lat = GeoUtils.getWorldLat(y);

    double x1 = 0.5 + Z14_WIDTH / 4;
    double lng1 = GeoUtils.getWorldLon(x1);
    double lng2 = GeoUtils.getWorldLon(x1 + Z14_WIDTH * 10d / 256);
    ForwardingProfile profile = new ForwardingProfile() {
      @Override
      public String name() {
        return "test";
      }
    };

    List<SourceFeature> featureList = new CopyOnWriteArrayList<>();
    profile.registerSourceHandler("osm", (in, features) -> featureList.add(in));
    profile.registerHandler((ForwardingProfile.FinishHandler) (name, featureCollectors, next) -> {
      if ("osm".equals(name)) {
        for (SourceFeature in : featureList) {
          var features = featureCollectors.get(in);
          features.point("layer")
            .setZoomRange(13, 14)
            .inheritAttrFromSource("a");
          for (var feature : features) {
            next.accept(feature);
          }
        }
      }
    });

    var results = runWithOsmElements(
      Map.of("threads", "1"),
      List.of(
        with(new OsmElement.Node(1, lat, lng1), t -> t.setTag("a", 1)),
        with(new OsmElement.Node(2, lat, lng2), t -> t.setTag("a", 3))
      ),
      profile
    );

    assertSubmap(sortListValues(Map.of(
      TileCoord.ofXYZ(Z14_TILES / 2, Z14_TILES / 2, 14), List.of(
        feature(newPoint(64, 128), Map.of("a", 1L)),
        feature(newPoint(74, 128), Map.of("a", 3L))
      ),
      TileCoord.ofXYZ(Z13_TILES / 2, Z13_TILES / 2, 13), List.of(
        // merge 32->37 and 37->42 since they have same attrs
        feature(newPoint(32, 64), Map.of("a", 1L)),
        feature(newPoint(37, 64), Map.of("a", 3L))
      )
    )), sortListValues(results.tiles));
  }

  private <K extends Comparable<? super K>, V extends List<?>> Map<K, ?> sortListValues(Map<K, V> input) {
    Map<K, List<?>> result = new TreeMap<>();
    for (var entry : input.entrySet()) {
      List<?> sorted = entry.getValue().stream().sorted(Comparator.comparing(Object::toString)).toList();
      result.put(entry.getKey(), sorted);
    }
    return result;
  }

  private Map.Entry<TileCoord, List<TestUtils.ComparableFeature>> newTileEntry(int x, int y, int z,
    List<TestUtils.ComparableFeature> features) {
    return Map.entry(TileCoord.ofXYZ(x, y, z), features);
  }

  private interface Runner {

    void run(FeatureGroup featureGroup, Profile profile, PlanetilerConfig config) throws Exception;
  }

  private interface LayerPostprocessFunction {

    List<VectorTile.Feature> process(String layer, int zoom, List<VectorTile.Feature> items)
      throws GeometryException;
  }

  private interface TilePostprocessFunction {

    Map<String, List<VectorTile.Feature>> process(TileCoord tileCoord, Map<String, List<VectorTile.Feature>> layers)
      throws GeometryException;
  }

  private record PlanetilerResults(
    Map<TileCoord, List<TestUtils.ComparableFeature>> tiles, Map<String, String> metadata, int tileDataCount
  ) {}

  private record TestProfile(
    @Override String name,
    @Override String description,
    @Override String attribution,
    @Override String version,
    BiConsumer<SourceFeature, FeatureCollector> processFeature,
    Function<OsmElement.Relation, List<OsmRelationInfo>> preprocessOsmRelation,
    LayerPostprocessFunction postprocessLayerFeatures,
    TilePostprocessFunction postprocessTileFeatures
  ) implements Profile {

    TestProfile(
      BiConsumer<SourceFeature, FeatureCollector> processFeature,
      Function<OsmElement.Relation, List<OsmRelationInfo>> preprocessOsmRelation,
      LayerPostprocessFunction postprocessLayerFeatures
    ) {
      this(TEST_PROFILE_NAME, TEST_PROFILE_DESCRIPTION, TEST_PROFILE_ATTRIBUTION, TEST_PROFILE_VERSION, processFeature,
        preprocessOsmRelation, postprocessLayerFeatures, null);
    }

    TestProfile(
      BiConsumer<SourceFeature, FeatureCollector> processFeature,
      Function<OsmElement.Relation, List<OsmRelationInfo>> preprocessOsmRelation,
      TilePostprocessFunction postprocessTileFeatures
    ) {
      this(TEST_PROFILE_NAME, TEST_PROFILE_DESCRIPTION, TEST_PROFILE_ATTRIBUTION, TEST_PROFILE_VERSION, processFeature,
        preprocessOsmRelation, null, postprocessTileFeatures);
    }

    static TestProfile processSourceFeatures(BiConsumer<SourceFeature, FeatureCollector> processFeature) {
      return new TestProfile(processFeature, (a) -> null, (a, b, c) -> c);
    }

    @Override
    public List<OsmRelationInfo> preprocessOsmRelation(
      OsmElement.Relation relation) {
      return preprocessOsmRelation == null ? null : preprocessOsmRelation.apply(relation);
    }

    @Override
    public void processFeature(SourceFeature sourceFeature, FeatureCollector features) {
      processFeature.accept(sourceFeature, features);
    }

    @Override
    public void release() {
    }

    @Override
    public List<VectorTile.Feature> postProcessLayerFeatures(String layer, int zoom,
      List<VectorTile.Feature> items) throws GeometryException {
      return postprocessLayerFeatures == null ? null : postprocessLayerFeatures.process(layer, zoom, items);
    }

    @Override
    public Map<String, List<VectorTile.Feature>> postProcessTileFeatures(TileCoord tileCoord,
      Map<String, List<VectorTile.Feature>> layers) throws GeometryException {
      return postprocessTileFeatures == null ? null : postprocessTileFeatures.process(tileCoord, layers);
    }
  }

  private static <T> List<T> orEmpty(List<T> in) {
    return in == null ? List.of() : in;
  }

  @Test
  void testBadRelation() throws Exception {
    // this threw an exception in OsmMultipolygon.build
    OsmXml osmInfo = TestUtils.readOsmXml("bad_spain_relation.xml");
    List<OsmElement> elements = convertToOsmElements(osmInfo);

    var results = runWithOsmElements(
      Map.of("threads", "1"),
      elements,
      (in, features) -> {
        if (in.hasTag("landuse", "forest")) {
          features.polygon("layer")
            .setZoomRange(12, 14)
            .setBufferPixels(4);
        }
      }
    );

    assertEquals(11, results.tiles.size());
  }

  private static List<OsmElement> convertToOsmElements(OsmXml osmInfo) {
    List<OsmElement> elements = new ArrayList<>();
    for (var node : orEmpty(osmInfo.nodes())) {
      var newNode = new OsmElement.Node(node.id(), node.lat(), node.lon());
      elements.add(newNode);
      for (var tag : orEmpty(node.tags())) {
        newNode.setTag(tag.k(), tag.v());
      }
    }
    for (var way : orEmpty(osmInfo.ways())) {
      var readerWay = new OsmElement.Way(way.id());
      elements.add(readerWay);
      for (var tag : orEmpty(way.tags())) {
        readerWay.setTag(tag.k(), tag.v());
      }
      for (var nodeRef : orEmpty(way.nodeRefs())) {
        readerWay.nodes().add(nodeRef.ref());
      }
    }
    for (var relation : orEmpty(osmInfo.relation())) {
      var readerRelation = new OsmElement.Relation(relation.id());
      elements.add(readerRelation);
      for (var tag : orEmpty(relation.tags())) {
        readerRelation.setTag(tag.k(), tag.v());
      }
      for (var member : orEmpty(relation.members())) {
        readerRelation.members().add(new OsmElement.Relation.Member(switch (member.type()) {
          case "way" -> OsmElement.Type.WAY;
          case "relation" -> OsmElement.Type.RELATION;
          case "node" -> OsmElement.Type.NODE;
          default -> throw new IllegalStateException("Unexpected value: " + member.type());
        }, member.ref(), member.role()));
      }
    }
    return elements;
  }

  @Test
  void testIssue496BaseballMultipolygon() throws Exception {
    // this generated a polygon that covered an entire z11 tile where the buffer intersected the baseball field
    OsmXml osmInfo = TestUtils.readOsmXml("issue_496_baseball_multipolygon.xml");
    List<OsmElement> elements = convertToOsmElements(osmInfo);

    var results = runWithOsmElements(
      Map.of("threads", "1"),
      elements,
      (in, features) -> {
        if (in.hasTag("natural", "sand")) {
          features.polygon("test")
            .setBufferPixels(4)
            .setPixelTolerance(0.5)
            .setMinPixelSize(0.1)
            .setAttr("id", in.id());
        }
      }
    );

    double areaAtZ14 = 20;

    for (var entry : results.tiles().entrySet()) {
      var tile = entry.getKey();
      for (var feature : entry.getValue()) {
        var geom = feature.geometry().geom();
        double area = geom.getArea();
        double expectedMaxArea = areaAtZ14 / (1 << (14 - tile.z()));
        assertTrue(area < expectedMaxArea, "tile=" + tile + " area=" + area + " geom=" + geom);
      }
    }

    assertEquals(8, results.tiles.size());
  }

  @Test
  void testIssue509LenaDelta() throws Exception {
    OsmXml osmInfo = TestUtils.readOsmXml("issue_509_lena_delta.xml");
    List<OsmElement> elements = convertToOsmElements(osmInfo);

    var results = runWithOsmElements(
      Map.of("threads", "1"),
      elements,
      (in, features) -> {
        if (in.hasTag("natural", "water")) {
          features.polygon("water").setAttr("id", in.id()).setMinZoom(10);
        }
      }
    );

    Map<Integer, Integer> counts = new TreeMap<>();
    for (var tile : results.tiles().keySet()) {
      counts.merge(tile.z(), 1, Integer::sum);
    }

    assertEquals(Map.of(
      10, 39,
      11, 125,
      12, 397,
      13, 1160,
      14, 3108
    ), counts);
  }

  @Test
  void testIssue546Terschelling() throws Exception {
    Geometry geometry = new WKBReader()
      .read(
        new InputStreamInStream(Files.newInputStream(TestUtils.pathToResource("issue_546_terschelling.wkb"))));
    geometry = GeoUtils.worldToLatLonCoords(geometry);

    assertNotNull(geometry);

    // automatically checks for self-intersections
    var results = runWithReaderFeatures(
      Map.of("threads", "1"),
      List.of(
        newReaderFeature(geometry, Map.of())
      ),
      (in, features) -> features.polygon("ocean")
        .setBufferPixels(4.0)
        .setMinZoom(0)
    );

    // this lat/lon is in the middle of an island and should not be covered by
    for (int z = 4; z <= 14; z++) {
      double lat = 53.391958;
      double lon = 5.2438441;

      var coord = TileCoord.aroundLngLat(lon, lat, z);
      var problematicTile = results.tiles.get(coord);
      if (z == 14) {
        assertNull(problematicTile);
        continue;
      }
      double scale = Math.pow(2, coord.z());

      double tileX = (GeoUtils.getWorldX(lon) * scale - coord.x()) * 256;
      double tileY = (GeoUtils.getWorldY(lat) * scale - coord.y()) * 256;

      var point = newPoint(tileX, tileY);

      assertEquals(1, problematicTile.size());
      var geomCompare = problematicTile.getFirst().geometry();
      geomCompare.validate();
      var geom = geomCompare.geom();

      assertFalse(geom.covers(point), "z" + z);
    }
  }

  private static TileArchiveConfig.Format extractFormat(String args) {

    final Optional<TileArchiveConfig.Format> format = Stream.of(TileArchiveConfig.Format.values())
      .filter(fmt -> args.contains("--output-format=" + fmt.id()))
      .findFirst();

    if (format.isPresent()) {
      return format.get();
    } else if (args.contains("--output-format=")) {
      throw new IllegalArgumentException("unhandled output format");
    } else {
      return TileArchiveConfig.Format.MBTILES;
    }
  }

  private static TileCompression extractTileCompression(String args) {
    if (args.contains("tile-compression=none")) {
      return TileCompression.NONE;
    } else if (args.contains("tile-compression=gzip")) {
      return TileCompression.GZIP;
    } else if (args.contains("tile-compression=")) {
      throw new IllegalArgumentException("unhandled tile compression");
    } else {
      return TileCompression.GZIP;
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {
    "",
    "--write-threads=2 --process-threads=2 --feature-read-threads=2 --threads=4",
    "--free-osm-after-read",
    "--compress-temp",
    "--osm-parse-node-bounds",
    "--output-format=pmtiles",
    "--output-format=csv",
    "--output-format=tsv",
    "--output-format=proto",
    "--output-format=pbf",
    "--output-format=json",
    "--output-format=files",
    "--tile-compression=none",
    "--tile-compression=gzip",
    "--output-layerstats",
    "--max-point-buffer=1"
  })
  void testPlanetilerRunner(String args) throws Exception {
    Path originalOsm = TestUtils.pathToResource("monaco-latest.osm.pbf");
    Path tempOsm = tempDir.resolve("monaco-temp.osm.pbf");
    final TileCompression tileCompression = extractTileCompression(args);

    final TileArchiveConfig.Format format = extractFormat(args);
    final String outputUri;
    final Path outputPath;
    switch (format) {
      case FILES -> {
        outputPath = tempDir.resolve("output");
        outputUri = outputPath.toString() + "?format=files";
      }
      default -> {
        outputPath = tempDir.resolve("output." + format.id());
        outputUri = outputPath.toString();
      }
    }

    final ReadableTileArchiveFactory readableTileArchiveFactory = switch (format) {
      case MBTILES -> Mbtiles::newReadOnlyDatabase;
      case CSV -> p -> InMemoryStreamArchive.fromCsv(p, ",");
      case TSV -> p -> InMemoryStreamArchive.fromCsv(p, "\t");
      case JSON -> InMemoryStreamArchive::fromJson;
      case PMTILES -> ReadablePmtiles::newReadFromFile;
      case PROTO, PBF -> InMemoryStreamArchive::fromProtobuf;
      case FILES -> p -> ReadableFilesArchive.newReader(p, Arguments.of());
    };

    Files.copy(originalOsm, tempOsm);
    Planetiler.create(Arguments.fromArgs(
        ("--tmpdir=" + tempDir.resolve("data") + " " + args).split("\\s+")
      ))
      .setProfile(new Profile.NullProfile() {
        @Override
        public void processFeature(SourceFeature source, FeatureCollector features) {
          if (source.canBePolygon() && source.hasTag("building", "yes")) {
            features.polygon("building").setZoomRange(0, 14).setMinPixelSize(1);
          } else if (source.isPoint() && source.hasTag("place")) {
            features.point("place").setZoomRange(0, 14);
          }
        }
      })
      .addOsmSource("osm", tempOsm)
      .addNaturalEarthSource("ne", TestUtils.pathToResource("natural_earth_vector.sqlite"))
      .addShapefileSource("shapefile", TestUtils.pathToResource("shapefile.zip"))
      .addGeoPackageSource("geopackage", TestUtils.pathToResource("geopackage.gpkg.zip"), null)
      .setOutput(outputUri)
      .run();

    // make sure it got deleted after write
    if (args.contains("free-osm-after-read")) {
      assertFalse(Files.exists(tempOsm));
    }

    try (var db = readableTileArchiveFactory.create(outputPath)) {
      int features = 0;
      var tileMap = TestUtils.getTileMap(db, tileCompression);
      for (var tile : tileMap.values()) {
        for (var feature : tile) {
          feature.geometry().validate();
          features++;
        }
      }

      int expectedFeatures = args.contains("max-point-buffer=1") ? 2311 : 2313;

      assertEquals(22, tileMap.size(), "num tiles");
      assertEquals(expectedFeatures, features, "num feature");

      final boolean checkMetadata = switch (format) {
        case MBTILES -> true;
        case PMTILES -> true;
        default -> db.metadata() != null;
      };

      if (checkMetadata) {
        assertSubmap(Map.of(
          "planetiler:version", BuildInfo.get().version(),
          "planetiler:osm:osmosisreplicationtime", "2021-04-21T20:21:46Z",
          "planetiler:osm:osmosisreplicationseq", "2947",
          "planetiler:osm:osmosisreplicationurl", "http://download.geofabrik.de/europe/monaco-updates"
        ), db.metadata().toMap());
      }
    }

    final Path layerstats = outputPath.resolveSibling(outputPath.getFileName().toString() + ".layerstats.tsv.gz");
    if (args.contains("--output-layerstats")) {
      assertTrue(Files.exists(layerstats));
      byte[] data = Files.readAllBytes(layerstats);
      byte[] uncompressed = Gzip.gunzip(data);
      String[] lines = new String(uncompressed, StandardCharsets.UTF_8).split("\n");
      assertEquals(33, lines.length);

      assertEquals(List.of(
        "z",
        "x",
        "y",
        "hilbert",
        "archived_tile_bytes",
        "layer",
        "layer_bytes",
        "layer_features",
        "layer_geometries",
        "layer_attr_bytes",
        "layer_attr_keys",
        "layer_attr_values"
      ), List.of(lines[0].split("\t")), lines[0]);

      var mapper = new CsvMapper();
      var reader = mapper
        .readerFor(Map.class)
        .with(CsvSchema.emptySchema().withColumnSeparator('\t').withLineSeparator("\n").withHeader());
      try (var items = reader.readValues(uncompressed)) {
        while (items.hasNext()) {
          @SuppressWarnings("unchecked") Map<String, String> next = (Map<String, String>) items.next();
          int z = Integer.parseInt(next.get("z"));
          int x = Integer.parseInt(next.get("x"));
          int y = Integer.parseInt(next.get("y"));
          int hilbert = Integer.parseInt(next.get("hilbert"));
          assertEquals(hilbert, TileCoord.ofXYZ(x, y, z).hilbertEncoded());
          assertTrue(Integer.parseInt(next.get("z")) <= 14, "bad z: " + next);
        }
      }

      // ensure tilestats standalone executable produces same output
      var standaloneLayerstatsOutput = tempDir.resolve("layerstats2.tsv.gz");
      TileSizeStats.main("--input=" + outputPath, "--output=" + standaloneLayerstatsOutput);
      byte[] standaloneData = Files.readAllBytes(standaloneLayerstatsOutput);
      byte[] standaloneUncompressed = Gzip.gunzip(standaloneData);
      assertEquals(
        new String(uncompressed, StandardCharsets.UTF_8),
        new String(standaloneUncompressed, StandardCharsets.UTF_8)
      );
    } else {
      assertFalse(Files.exists(layerstats));
    }
  }

  @Test
  void testPlanetilerRunnerShapefile() throws Exception {
    Path mbtiles = tempDir.resolve("output.mbtiles");
    Path resourceDir = TestUtils.pathToResource("");

    Planetiler.create(Arguments.fromArgs("--tmpdir=" + tempDir.resolve("data")))
      .setProfile(new Profile.NullProfile() {
        @Override
        public void processFeature(SourceFeature source, FeatureCollector features) {
          features.point("stations")
            .setZoomRange(0, 14)
            .setAttr("source", source.getSource())
            .setAttr("layer", source.getSourceLayer());
        }
      })
      // Match *.shp within [shapefile.zip, shapefile-copy.zip]
      .addShapefileGlobSource("shapefile-glob", resourceDir, "shape*.zip")
      // Match *.shp within shapefile.zip
      .addShapefileGlobSource("shapefile-glob-zip", resourceDir.resolve("shapefile.zip"), "*.shp")
      // Match *.shp within shapefile.zip
      .addShapefileSource("shapefile", resourceDir.resolve("shapefile.zip"))
      .setOutput(mbtiles)
      .run();

    try (Mbtiles db = Mbtiles.newReadOnlyDatabase(mbtiles)) {
      long fileCount = 0, globCount = 0, globZipCount = 0;
      var tileMap = TestUtils.getTileMap(db);
      for (var tile : tileMap.values()) {
        for (var feature : tile) {
          feature.geometry().validate();
          assertEquals("stations", feature.attrs().get("layer"));
          switch ((String) feature.attrs().get("source")) {
            case "shapefile" -> fileCount++;
            case "shapefile-glob" -> globCount++;
            case "shapefile-glob-zip" -> globZipCount++;
          }
        }
      }

      assertTrue(fileCount > 0);
      // `shapefile` and `shapefile-glob-zip` both match only one file.
      assertEquals(fileCount, globZipCount);
      // `shapefile-glob` matches two input files, should have 2x number of features of `shapefile`.
      assertEquals(2 * fileCount, globCount);
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {
    "",
    "--write-threads=2 --process-threads=2 --feature-read-threads=2 --threads=4",
    "--input-file=geopackage.gpkg"
  })
  void testPlanetilerRunnerGeoPackage(String args) throws Exception {
    Path mbtiles = tempDir.resolve("output.mbtiles");
    String inputFile = Arguments.fromArgs(args).getString("input-file", "", "geopackage.gpkg.zip");

    Planetiler.create(Arguments.fromArgs((args + " --tmpdir=" + tempDir.resolve("data")).split("\\s+")))
      .setProfile(new Profile.NullProfile() {
        @Override
        public void processFeature(SourceFeature source, FeatureCollector features) {
          features.point("stations")
            .setZoomRange(0, 14)
            .setAttr("name", source.getString("name"));
        }
      })
      .addGeoPackageSource("geopackage", TestUtils.pathToResource(inputFile), null)
      .setOutput(mbtiles)
      .run();

    try (Mbtiles db = Mbtiles.newReadOnlyDatabase(mbtiles)) {
      Set<String> uniqueNames = new HashSet<>();
      long featureCount = 0;
      var tileMap = TestUtils.getTileMap(db);
      for (var tile : tileMap.values()) {
        for (var feature : tile) {
          feature.geometry().validate();
          featureCount++;
          uniqueNames.add((String) feature.attrs().get("name"));
        }
      }

      assertTrue(featureCount > 0);
      assertEquals(86, uniqueNames.size());
      assertTrue(uniqueNames.contains("Van Dörn Street"));
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {
    " --small_Feat_Strategy=square --minzoom=14 --maxzoom=14 --pixelation_grid_size_overrides=12=512 "
      + "--output=H:/100数据/Parquet\\default-14\\default-14.mbtiles"
      + " --is_rasterize=true --pixelation_zoom=-1  --rasterize_min_zoom=0 --rasterize_max_zoom=14"
      + " --outputType=mbtiles  --temp_nodes=F:\\test --temp_multipolygons=E:\\test --tile_weights=D:\\Project\\Java\\server-code\\src\\main\\resources\\planetiler\\tile_weights.tsv.gz "
      + " -oosSavePath=H:\\100数据\\output --oosCorePoolSize=4 --oosMaxPoolSize=4 --bucketName=linespace --accessKey=linespace_test --secretKey=linespace_test --endpoint=http://123.139.158.75:9325 --force",
  })
  void testPlanetilerRunnerParquetDLTB(String args) throws Exception {
//    String basePath = "E:\\Linespace\\SceneMapServer\\Data\\parquet\\xian";
//    String tempDir = basePath + "\\mergeSmallFeatures";
//    String outputPath = basePath + "\\mergeSmallFeatures\\mergeSmallFeatures.mbtiles";
//    List<Path> inputPaths = Stream.of(basePath + "\\xian.parquet").map(Paths::get).toList();
    String basePath = "H:/100数据/Parquet/";
    String tempDir = basePath + "\\default-14";
    String outputPath = basePath + "\\default-14\\default-14.mbtiles";
    List<Path> inputPaths = Stream.of(basePath + "\\dltb.parquet").map(Paths::get).toList();

    Planetiler planetiler = Planetiler.create(Arguments.fromArgs(
      (args + " --tmpdir=" + tempDir).split("\\s+")));
    PlanetilerConfig config = planetiler.config();
    planetiler
      .setProfile((source, features) -> {
        try {
          FeatureCollector.Feature feature = features.anyGeometry("linespace_layer")
            .setSortKey(SortKey
              .orderByDouble(source.area(), 1d, 0, 1 << (SORT_KEY_BITS - 7) - 1)
              .get())
            .setPointLabelGridPixelSize(createLabelGridSizeFunction())
            .setPointLabelGridLimit(createLabelGridLimitFunction())
            //              .setBufferPixelOverrides(createBufferPixelFunction())
            .setPixelToleranceAtAllZooms(0)
            .setMinPixelSizeAtAllZooms(0);

          source.tags().forEach(feature::setAttr);
        } catch (GeometryException e) {
          throw new RuntimeException(e);
        }
      })
      .addParquetSource("parquet", inputPaths, false, null, props -> props.get("linespace_layer"))
      .setOutput(outputPath)
      .run();
  }

  @ParameterizedTest
  @ValueSource(strings = {
    " --small_Feat_Strategy=square --minzoom=14 --maxzoom=14 --pixelation_grid_size_overrides=12=512 "
      + "--output=H:/100数据/Parquet\\guangdong-14\\guangdong-14.mbtiles"
      + " --is_rasterize=true --pixelation_zoom=-1  --rasterize_min_zoom=0 --rasterize_max_zoom=14"
      + " --outputType=mbtiles  --temp_nodes=F:\\test --temp_multipolygons=E:\\test --tile_weights=D:\\Project\\Java\\server-code\\src\\main\\resources\\planetiler\\tile_weights.tsv.gz "
      + " -oosSavePath=H:\\100数据\\output --oosCorePoolSize=4 --oosMaxPoolSize=4 --bucketName=linespace --accessKey=linespace_test --secretKey=linespace_test --endpoint=http://123.139.158.75:9325 --force",
  })
  void testPlanetilerRunnerParquetGuandong(String args) throws Exception {
//    String basePath = "E:\\Linespace\\SceneMapServer\\Data\\parquet\\xian";
//    String tempDir = basePath + "\\mergeSmallFeatures";
//    String outputPath = basePath + "\\mergeSmallFeatures\\mergeSmallFeatures.mbtiles";
//    List<Path> inputPaths = Stream.of(basePath + "\\xian.parquet").map(Paths::get).toList();
    String basePath = "H:/100数据/Parquet/";
    String tempDir = basePath + "\\guangdong-14";
    String outputPath = basePath + "\\guangdong-14\\guangdong-14.mbtiles";
    List<Path> inputPaths = Stream.of(basePath + "\\guangdong-latest-multipolygons.parquet").map(Paths::get).toList();

    Planetiler planetiler = Planetiler.create(Arguments.fromArgs(
      (args + " --tmpdir=" + tempDir).split("\\s+")));
    PlanetilerConfig config = planetiler.config();
    planetiler
      .setProfile((source, features) -> {
        try {
          FeatureCollector.Feature feature = features.anyGeometry("linespace_layer")
            .setSortKey(SortKey
              .orderByDouble(source.area(), 1d, 0, 1 << (SORT_KEY_BITS - 7) - 1)
              .get())
            .setPointLabelGridPixelSize(createLabelGridSizeFunction())
            .setPointLabelGridLimit(createLabelGridLimitFunction())
            //              .setBufferPixelOverrides(createBufferPixelFunction())
            .setPixelToleranceAtAllZooms(0)
            .setMinPixelSizeAtAllZooms(0);

          source.tags().forEach(feature::setAttr);
        } catch (GeometryException e) {
          throw new RuntimeException(e);
        }
      })
      .addParquetSource("parquet", inputPaths, false, null, props -> props.get("linespace_layer"))
      .setOutput(outputPath)
      .run();
  }

  @ParameterizedTest
  @ValueSource(strings = {
    " --minzoom=0 --maxzoom=14 "
//      + "--output=E:\\Linespace\\SceneMapServer\\Data\\parquet\\矢量\\polygon-华山_建筑\\default.mbtiles"
      + " --is_rasterize=false --pixelation_zoom=-1  --rasterize_min_zoom=0 --rasterize_max_zoom=14"
      + " --outputType=mbtiles  --temp_nodes=F:\\test --temp_multipolygons=E:\\test --tile_weights=D:\\Project\\Java\\server-code\\src\\main\\resources\\planetiler\\tile_weights.tsv.gz  --force"
//      + " -oosSavePath=E:\\Linespace\\SceneMapServer\\Data\\parquet --oosCorePoolSize=4 --oosMaxPoolSize=4 --bucketName=linespace --accessKey=linespace_test --secretKey=linespace_test --endpoint=http://123.139.158.75:9325 ",
  })
  void testPlanetilerRunnerParquetGeometryType(String args) throws Exception {
    String basePath = "E:\\Linespace\\SceneMapServer\\Data\\parquet\\矢量";
    String tempDir = basePath + "\\line-HS_L_4525-1";
    String outputPath = basePath + "\\line-HS_L_4525-1\\default.mbtiles";
    List<Path> inputPaths = Stream.of(basePath + "\\line-HS_L_4525-1.parquet").map(Paths::get).toList();

    Planetiler planetiler = Planetiler.create(Arguments.fromArgs(
      (args + " --tmpdir=" + tempDir).split("\\s+")));
    PlanetilerConfig config = planetiler.config();
    Map<String, HashSet<String>> geomTypes = new ConcurrentHashMap<>();
    planetiler
      .setProfile((source, features) -> {
        try {
          FeatureCollector.Feature feature = features.anyGeometry("linespace_layer")
            .setSortKey(SortKey
              .orderByDouble(source.area(), 1d, 0, 1 << (SORT_KEY_BITS - 7) - 1)
              .get())
            .setPointLabelGridPixelSize(createLabelGridSizeFunction())
            .setPointLabelGridLimit(createLabelGridLimitFunction())
            //              .setBufferPixelOverrides(createBufferPixelFunction())
            .setPixelToleranceAtAllZooms(0)
            .setMinPixelSizeAtAllZooms(0);

          String geometryType = null;
          if (source.isPoint()) {
            geometryType = GeometryType.POINT.name();
          } else if (source.canBePolygon()) {
            geometryType = GeometryType.POLYGON.name();
          } else if (source.canBeLine()) {
            geometryType = GeometryType.LINE.name();
          }

          if (StringUtils.isNotBlank(geometryType)) {
            geomTypes.computeIfAbsent(feature.getLayer(), k -> new HashSet<>()).add(geometryType);
          }
          source.tags().forEach(feature::setAttr);
        } catch (GeometryException e) {
          throw new RuntimeException(e);
        }
      })
      .addParquetSource("parquet", inputPaths, false, null, props -> props.get("linespace_layer"))
      .setOutput(outputPath)
      .run();

    // 数据写入json
    try (Mbtiles mbtiles = (Mbtiles) TileArchives.newWriter(Paths.get(outputPath), config)) {
      updateMetadata(mbtiles, geomTypes);
    }
  }


  @ParameterizedTest
  @ValueSource(strings = {
    " --minzoom=0 --maxzoom=14 "
      + " --output=G:\\数据\\六大类数据\\1.矢量\\parquet\\陕西POI\\POI\\default.mbtiles "
      + "  --label_grid_pixel_size=12=8,5=1 --label_grid_limit=12=10 --layer_name=shanxi-poi "
      + " --outputType=mbtiles  --temp_nodes=F:\\test --temp_multipolygons=E:\\test --tile_weights=D:\\Project\\Java\\server-code\\src\\main\\resources\\planetiler\\tile_weights.tsv.gz -oosSavePath=E:\\Linespace\\SceneMapServer\\Data --oosCorePoolSize=4 --oosMaxPoolSize=4 --bucketName=linespace --accessKey=linespace_test --secretKey=linespace_test --endpoint=http://123.139.158.75:9325 --force",
  })
  void testPlanetilerRunnerParquetPOI(String args) throws Exception {
    String basePath = "G:\\数据\\六大类数据\\1.矢量\\parquet\\陕西POI";
    String tempDir = basePath + "\\POI";
    String outputPath = basePath + "\\POI\\default.mbtiles";
    List<Path> inputPaths = Stream.of(basePath + "\\陕西POI.parquet").map(Paths::get).toList();

    Map<String, Map<String, Integer>> zoomLevelMap = new HashMap<>();
    Map<String, Integer> mainClassMap = new HashMap<>();
    mainClassMap.put("交通设施", 0);
    mainClassMap.put("医疗保健", 1);
    mainClassMap.put("汽车相关", 2);
    mainClassMap.put("餐饮美食", 4);
    mainClassMap.put("生活服务", 5);
    mainClassMap.put("旅游景点", 6);
    mainClassMap.put("住宿酒店", 7);
    mainClassMap.put("购物消费", 8);
    mainClassMap.put("金融机构", 9);
    mainClassMap.put("运动健身", 10);
    mainClassMap.put("公司企业", 11);
    mainClassMap.put("休闲娱乐", 12);
    mainClassMap.put("商务住宅", 13);
    mainClassMap.put("科教文化", 14);
    zoomLevelMap.put("大类", mainClassMap);

    Map<String, Map<String, Integer>> priorityLevelsMap = new HashMap<>();
    Map<String, Integer> mediumClassMap = new HashMap<>();
    mediumClassMap.put("飞机", 10);
    mediumClassMap.put("公交站", 50);
    mediumClassMap.put("火车", 20);
    mediumClassMap.put("港口码头", 40);
    mediumClassMap.put("地铁", 30);
    priorityLevelsMap.put("中类", mediumClassMap);

    Poi.PoiTilingConfig poiTilingConfig = new Poi.PoiTilingConfig(priorityLevelsMap, zoomLevelMap, null, false);

    Planetiler planetiler = Planetiler.create(Arguments.fromArgs(
      (args + " --tmpdir=" + tempDir).split("\\s+")));
    PlanetilerConfig config = planetiler.config();
    planetiler.setProfile(new Poi(config, poiTilingConfig))
      .addParquetSource("parquet", inputPaths, false, null, props -> props.get("linespace_layer"))
      .setOutput(outputPath)
      .run();
  }


  private void updateMetadata(Mbtiles mbtiles, Map<String, HashSet<String>> geomTypes) {
    Mbtiles.Metadata metadata = mbtiles.metadataTable();
    TileArchiveMetadata archiveMetadata = metadata.get();
    TileArchiveMetadata.TileArchiveMetadataJson metadataJson = TileArchiveMetadata.TileArchiveMetadataJson.create(
      archiveMetadata.json().vectorLayers().stream()
        .map(vectorLayer -> vectorLayer.withGeometryTypes(geomTypes.get(vectorLayer.id()))).toList());
    metadata.updateMetadata(Map.of("json", JsonUitls.toJsonString(metadataJson)));
  }

  @ParameterizedTest
  @ValueSource(strings = {
    " --minzoom=0 --maxzoom=14 "
      + " --small_Feat_Strategy=square --merge_fields=DLBM --min_dist_sizes=5=0.0625,6=0.09375,7=0.125,8=0.1875,9=0.25,10=0.375,11=0.4375,12=0.5 "
      + " --output=E:\\Linespace\\SceneMapServer\\Data\\parquet\\dltb\\合并填充\\合并填充.mbtiles "
//      + "  --pixelation_grid_size_overrides=12=512 --is_rasterize=false --pixelation_zoom=12  --rasterize_min_zoom=12 --rasterize_max_zoom=13"
      + " --outputType=mbtiles  --temp_nodes=F:\\test --temp_multipolygons=E:\\test --tile_weights=D:\\Project\\Java\\server-code\\src\\main\\resources\\planetiler\\tile_weights.tsv.gz -oosSavePath=E:\\Linespace\\SceneMapServer\\Data --oosCorePoolSize=4 --oosMaxPoolSize=4 --bucketName=linespace --accessKey=linespace_test --secretKey=linespace_test --endpoint=http://123.139.158.75:9325 --force",
  })
  void testPlanetilerRunnerParquet(String args) throws Exception {
    String basePath = "E:\\Linespace\\SceneMapServer\\Data\\parquet\\dltb";
    String tempDir = basePath + "\\合并填充";
    String outputPath = basePath + "\\合并填充\\合并填充.mbtiles";
    List<Path> inputPaths = Stream.of(basePath + "\\dltb.parquet").map(Paths::get).toList();

    Planetiler planetiler = Planetiler.create(Arguments.fromArgs(
      (args + " --tmpdir=" + tempDir).split("\\s+")));
    PlanetilerConfig config = planetiler.config();
    planetiler
      .setProfile(new Profile() {
        @Override
        public void processFeature(SourceFeature source, FeatureCollector features) {
          try {
            FeatureCollector.Feature feature = features.anyGeometry("linespace_layer")
              .setSortKey(SortKey
                .orderByDouble(source.area(), 1d, 0, 1 << (SORT_KEY_BITS - 7) - 1)
                .get())
              .setPointLabelGridPixelSize(createLabelGridSizeFunction())
              .setPointLabelGridLimit(createLabelGridLimitFunction())
              .setPixelToleranceAtAllZooms(0.0625)
              .setMinPixelSizeAtAllZooms(0);
            source.tags().forEach(feature::setAttr);
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        }

        @Override
        public Map<String, List<VectorTile.Feature>> postProcessTileFeatures(TileCoord tileCoord,
          Map<String, List<VectorTile.Feature>> layers) throws GeometryException {

          return layers;
        }

        @Override
        public List<VectorTile.Feature> postProcessLayerFeatures(String layer, int zoom,
          List<VectorTile.Feature> items) throws GeometryException {
          if (config.mergeFields() == null) {
            return items;
          }

          double minDistAndBuffer = ZoomFunction.applyAsDoubleOrElse(createMinDistSizesFunction(), zoom, 0);
          if (minDistAndBuffer == 0) {
            return items;
          }

          List<VectorTile.Feature> features = FeatureMerge.mergeNearbyPolygonsAndFill(items, 0, 0, minDistAndBuffer,
            minDistAndBuffer,
            config.mergeFields());

//          features.sort(Comparator.comparingDouble(o -> (Integer) o.getTag("Shape_Area")));
//          // 限制大小
//          features.stream().map(feature -> {
//            feature.
//          })
          return features;
        }
      })
      .addParquetSource("parquet", inputPaths, false, null, props -> props.get("linespace_layer"))
      .setOutput(outputPath)
      .run();
  }


  private ZoomFunction<Number> createLabelGridSizeFunction() {
    Map<Integer, Number> map = new HashMap<>();
    map.put(12, 2d);
//    map.put(8, 1d / 4d);
//    map.put(6, 1d / 8d);
//    map.put(1, 1d / 7d);
    return ZoomFunction.fromMaxZoomThresholds(map);
  }

  private ZoomFunction<Number> createLabelGridLimitFunction() {
    Map<Integer, Number> map = new HashMap<>();
    map.put(12, 13d);
//    map.put(8, 2d);
//    map.put(6, 2d);
//    map.put(1, 2d);
    return ZoomFunction.fromMaxZoomThresholds(map);
  }

  // 建议的改进方案
  private ZoomFunction<Number> createMinDistSizesFunction() {
    return ZoomFunction.fromMaxZoomThresholds(Map.of(
      5, 0.01,
      8, 0.05,
      12, 0.1
    ));
  }

  private void runWithProfile(Path tempDir, Profile profile, boolean force) throws Exception {
    Planetiler.create(Arguments.of("tmpdir", tempDir, "force", Boolean.toString(force)))
      .setProfile(profile)
      .addOsmSource("osm", TestUtils.pathToResource("monaco-latest.osm.pbf"))
      .addNaturalEarthSource("ne", TestUtils.pathToResource("natural_earth_vector.sqlite"))
      .addShapefileSource("shapefile", TestUtils.pathToResource("shapefile.zip"))
      .addGeoPackageSource("geopackage", TestUtils.pathToResource("geopackage.gpkg.zip"), null)
      .setOutput(tempDir.resolve("output.mbtiles"))
      .run();
  }

  @Test
  void testPlanetilerMemoryCheck() {
    assertThrows(Exception.class, () -> runWithProfile(tempDir, new Profile.NullProfile() {
        @Override
        public long estimateIntermediateDiskBytes(long osmSize) {
          return Long.MAX_VALUE / 10L;
        }
      }, false)
    );
    assertThrows(Exception.class, () -> runWithProfile(tempDir, new Profile.NullProfile() {
        @Override
        public long estimateOutputBytes(long osmSize) {
          return Long.MAX_VALUE / 10L;
        }
      }, false)
    );
    assertThrows(Exception.class, () -> runWithProfile(tempDir, new Profile.NullProfile() {
        @Override
        public long estimateRamRequired(long osmSize) {
          return Long.MAX_VALUE / 10L;
        }
      }, false)
    );
  }

  @Test
  void testPlanetilerMemoryCheckForce() throws Exception {
    runWithProfile(tempDir, new Profile.NullProfile() {
      @Override
      public long estimateIntermediateDiskBytes(long osmSize) {
        return Long.MAX_VALUE / 10L;
      }
    }, true);
    runWithProfile(tempDir, new Profile.NullProfile() {
      @Override
      public long estimateOutputBytes(long osmSize) {
        return Long.MAX_VALUE / 10L;
      }
    }, true);
    runWithProfile(tempDir, new Profile.NullProfile() {
      @Override
      public long estimateRamRequired(long osmSize) {
        return Long.MAX_VALUE / 10L;
      }
    }, true);
  }

  @Test
  void testHandleProfileException() throws Exception {
    var results = runWithOsmElements(
      Map.of("threads", "1"),
      List.of(
        with(new OsmElement.Node(1, 0, 0), t -> t.setTag("attr", "value"))
      ),
      (in, features) -> {
        throw new IllegalStateException("intentional exception!") {

          // suppress stack trace in logs
          @Override
          public synchronized Throwable fillInStackTrace() {
            return this;
          }
        };
      }
    );

    assertSubmap(Map.of(), results.tiles);
  }


  private PlanetilerResults runForCompactTest(boolean compactDbEnabled) throws Exception {
    return runWithReaderFeatures(
      Map.of("threads", "1", "mbtiles-compact", Boolean.toString(compactDbEnabled)),
      List.of(
        newReaderFeature(WORLD_POLYGON, Map.of())
      ),
      (in, features) -> features.polygon("layer")
        .setZoomRange(0, 2)
        .setBufferPixels(0)
    );
  }

  @Test
  void testCompactDb() throws Exception {

    var compactResult = runForCompactTest(true);
    var nonCompactResult = runForCompactTest(false);

    assertEquals(nonCompactResult.tiles, compactResult.tiles);
    assertTrue(
      compactResult.tileDataCount() < compactResult.tiles.size(),
      "tileDataCount=%s should be less than tileCount=%s".formatted(
        compactResult.tileDataCount(), compactResult.tiles.size()
      )
    );
  }


  private PlanetilerResults runForMaxZoomTest(Map<String, String> attrs) throws Exception {
    double z8Pixel = 1d / (1 << 8) / 256d;
    double tileMiddle = 0.5 + Z14_WIDTH / 2;
    return runWithReaderFeatures(
      attrs,
      List.of(
        newReaderFeature(newLineString(worldCoordinateList(
          tileMiddle, tileMiddle,
          tileMiddle + z8Pixel / 2, tileMiddle
        )), Map.of())
      ),
      (in, features) -> features.line("layer").setBufferPixels(0).setZoomRange(0, 14)
    );
  }

  @Test
  void testRenderMaxzoom() throws Exception {

    var baseResult = runForMaxZoomTest(Map.of(
      "minzoom", "0",
      "maxzoom", "14")
    );
    var maxzoomResult = runForMaxZoomTest(Map.of(
      "minzoom", "0",
      "maxzoom", "8"
    ));
    var renderMaxzoomResult = runForMaxZoomTest(Map.of(
      "minzoom", "0",
      "maxzoom", "8",
      "render_maxzoom", "8"
    ));

    // the feature is too small to include at z8, unless z8 is the max zoom for rendering to support overzooming
    var z8Tile = TileCoord.ofXYZ((1 << 8) / 2, (1 << 8) / 2, 8);
    assertFalse(baseResult.tiles.containsKey(z8Tile));
    assertTrue(renderMaxzoomResult.tiles.containsKey(z8Tile));
    assertFalse(maxzoomResult.tiles.containsKey(z8Tile));
  }

  private PlanetilerResults runForBoundsTest(int minzoom, int maxzoom, String key, String value) throws Exception {
    return runWithReaderFeatures(
      Map.of("threads", "1", key, value),
      List.of(
        newReaderFeature(WORLD_POLYGON, Map.of())
      ),
      (in, features) -> features.polygon("layer")
        .setZoomRange(minzoom, maxzoom)
        .setBufferPixels(0)
    );
  }

  @Test
  void testBoundFilters() throws Exception {
    var origResult = runForBoundsTest(0, 2, "", "");
    var bboxResult = runForBoundsTest(0, 2, "bounds", "1,-85.05113,180,-1");
    var polyResult = runForBoundsTest(0, 2, "polygon", TestUtils.pathToResource("bottomrightearth.poly").toString());

    assertEquals(1 + 4 + 16, origResult.tiles.size());
    assertEquals(Set.of(
      TileCoord.ofXYZ(0, 0, 0),
      TileCoord.ofXYZ(1, 1, 1),
      TileCoord.ofXYZ(2, 2, 2),
      TileCoord.ofXYZ(3, 2, 2),
      TileCoord.ofXYZ(2, 3, 2),
      TileCoord.ofXYZ(3, 3, 2)
    ), bboxResult.tiles.keySet());
    assertEquals(Set.of(
      TileCoord.ofXYZ(0, 0, 0),
      TileCoord.ofXYZ(1, 1, 1),
      // TileCoord.ofXYZ(2, 2, 2),  - omit since this one is outside of triangle
      TileCoord.ofXYZ(3, 2, 2),
      TileCoord.ofXYZ(2, 3, 2),
      TileCoord.ofXYZ(3, 3, 2)
    ), polyResult.tiles.keySet());

    // but besides the omitted tile, the rest should be the same
    bboxResult.tiles.remove(TileCoord.ofXYZ(2, 2, 2));
    assertEquals(bboxResult.tiles, polyResult.tiles);
  }

  @Test
  void testSimplePolygon() throws Exception {
    List<Coordinate> points = z14PixelRectangle(0, 40);

    var results = runWithReaderFeatures(
      Map.of("threads", "1"),
      List.of(
        newReaderFeature(newPolygon(points), Map.of())
      ),
      (in, features) -> features.polygon("layer")
        .setZoomRange(0, 14)
        .setBufferPixels(0)
        .setMinPixelSize(10) // should only show up z14 (40) z13 (20) and z12 (10)
    );

    assertEquals(Map.ofEntries(
      newTileEntry(Z12_TILES / 2, Z12_TILES / 2, 12, List.of(
        feature(newPolygon(rectangleCoordList(0, 10)), Map.of())
      )),
      newTileEntry(Z13_TILES / 2, Z13_TILES / 2, 13, List.of(
        feature(newPolygon(rectangleCoordList(0, 20)), Map.of())
      )),
      newTileEntry(Z14_TILES / 2, Z14_TILES / 2, 14, List.of(
        feature(newPolygon(rectangleCoordList(0, 40)), Map.of())
      ))
    ), results.tiles);
  }

  @Test
  void testCentroidWithPolygonMinSize() throws Exception {
    List<Coordinate> points = z14PixelRectangle(0, 40);

    var results = runWithReaderFeatures(
      Map.of("threads", "1"),
      List.of(
        newReaderFeature(newPolygon(points), Map.of())
      ),
      (in, features) -> features.centroid("layer")
        .setZoomRange(0, 14)
        .setBufferPixels(0)
        .setMinPixelSize(10) // should only show up z14 (40) z13 (20) and z12 (10)
    );

    assertEquals(Map.ofEntries(
      newTileEntry(Z12_TILES / 2, Z12_TILES / 2, 12, List.of(
        feature(newPoint(5, 5), Map.of())
      )),
      newTileEntry(Z13_TILES / 2, Z13_TILES / 2, 13, List.of(
        feature(newPoint(10, 10), Map.of())
      )),
      newTileEntry(Z14_TILES / 2, Z14_TILES / 2, 14, List.of(
        feature(newPoint(20, 20), Map.of())
      ))
    ), results.tiles);
  }

  @Test
  void testCentroidWithLineMinSize() throws Exception {
    List<Coordinate> points = z14CoordinatePixelList(0, 4, 40, 4);

    var results = runWithReaderFeatures(
      Map.of("threads", "1"),
      List.of(
        newReaderFeature(newLineString(points), Map.of())
      ),
      (in, features) -> features.centroid("layer")
        .setZoomRange(0, 14)
        .setBufferPixels(0)
        .setMinPixelSize(10) // should only show up z14 (40) z13 (20) and z12 (10)
    );

    assertEquals(Map.ofEntries(
      newTileEntry(Z12_TILES / 2, Z12_TILES / 2, 12, List.of(
        feature(newPoint(5, 1), Map.of())
      )),
      newTileEntry(Z13_TILES / 2, Z13_TILES / 2, 13, List.of(
        feature(newPoint(10, 2), Map.of())
      )),
      newTileEntry(Z14_TILES / 2, Z14_TILES / 2, 14, List.of(
        feature(newPoint(20, 4), Map.of())
      ))
    ), results.tiles);
  }

  @Test
  void testAttributeMinSizeLine() throws Exception {
    List<Coordinate> points = z14CoordinatePixelList(0, 4, 40, 4);

    var results = runWithReaderFeatures(
      Map.of("threads", "1"),
      List.of(
        newReaderFeature(newLineString(points), Map.of())
      ),
      (in, features) -> features.line("layer")
        .setZoomRange(11, 14)
        .setBufferPixels(0)
        .setAttrWithMinSize("a", "1", 10)
        .setAttrWithMinSize("b", "2", 20)
        .setAttrWithMinSize("c", "3", 40)
        .setAttrWithMinSize("d", "4", 40, 0, 13) // should show up at z13 and above
    );

    assertEquals(Map.ofEntries(
      newTileEntry(Z11_TILES / 2, Z11_TILES / 2, 11, List.of(
        feature(newLineString(0, 0.5, 5, 0.5), Map.of())
      )),
      newTileEntry(Z12_TILES / 2, Z12_TILES / 2, 12, List.of(
        feature(newLineString(0, 1, 10, 1), Map.of("a", "1"))
      )),
      newTileEntry(Z13_TILES / 2, Z13_TILES / 2, 13, List.of(
        feature(newLineString(0, 2, 20, 2), Map.of("a", "1", "b", "2", "d", "4"))
      )),
      newTileEntry(Z14_TILES / 2, Z14_TILES / 2, 14, List.of(
        feature(newLineString(0, 4, 40, 4), Map.of("a", "1", "b", "2", "c", "3", "d", "4"))
      ))
    ), results.tiles);
  }

  @Test
  void testAttributeMinSizePoint() throws Exception {
    List<Coordinate> points = z14CoordinatePixelList(0, 4, 40, 4);

    var results = runWithReaderFeatures(
      Map.of("threads", "1"),
      List.of(
        newReaderFeature(newLineString(points), Map.of())
      ),
      (in, features) -> features.centroid("layer")
        .setZoomRange(11, 14)
        .setBufferPixels(0)
        .setAttrWithMinSize("a", "1", 10)
        .setAttrWithMinSize("b", "2", 20)
        .setAttrWithMinSize("c", "3", 40)
        .setAttrWithMinSize("d", "4", 40, 0, 13) // should show up at z13 and above
    );

    assertEquals(Map.ofEntries(
      newTileEntry(Z11_TILES / 2, Z11_TILES / 2, 11, List.of(
        feature(newPoint(2.5, 0.5), Map.of())
      )),
      newTileEntry(Z12_TILES / 2, Z12_TILES / 2, 12, List.of(
        feature(newPoint(5, 1), Map.of("a", "1"))
      )),
      newTileEntry(Z13_TILES / 2, Z13_TILES / 2, 13, List.of(
        feature(newPoint(10, 2), Map.of("a", "1", "b", "2", "d", "4"))
      )),
      newTileEntry(Z14_TILES / 2, Z14_TILES / 2, 14, List.of(
        feature(newPoint(20, 4), Map.of("a", "1", "b", "2", "c", "3", "d", "4"))
      ))
    ), results.tiles);
  }

  @Test
  void testBoundFiltersFill() throws Exception {
    var polyResultz8 = runForBoundsTest(8, 8, "polygon", TestUtils.pathToResource("bottomrightearth.poly").toString());

    int z8tiles = 1 << 8;
    assertFalse(polyResultz8.tiles.containsKey(TileCoord.ofXYZ(z8tiles * 3 / 4, z8tiles * 5 / 8, 8)));
    assertTrue(polyResultz8.tiles.containsKey(TileCoord.ofXYZ(z8tiles * 3 / 4, z8tiles * 7 / 8, 8)));
  }

  @FunctionalInterface
  private interface ReadableTileArchiveFactory {

    ReadableTileArchive create(Path p) throws IOException;
  }

  @Test
  void testLabelGridLimitLine() throws Exception {
    double y = 0.5 + Z14_WIDTH / 4;
    double lat = GeoUtils.getWorldLat(y);

    double x1 = 0.5 + Z14_WIDTH / 4;
    double lng1 = GeoUtils.getWorldLon(x1);
    double lng2 = GeoUtils.getWorldLon(x1 + Z14_WIDTH * 10d / 256);
    double lng3 = GeoUtils.getWorldLon(x1 + Z14_WIDTH * 20d / 256);
    double lng4 = GeoUtils.getWorldLon(x1 + Z14_WIDTH * 30d / 256);

    var results = runWithReaderFeatures(
      Map.of("threads", "1"),
      List.of(
        newReaderFeature(newLineString(lng1, lat, lng2, lat), Map.of("rank", "1")),
        newReaderFeature(newLineString(lng2, lat, lng3, lat), Map.of("rank", "2")),
        newReaderFeature(newLineString(lng3, lat, lng4, lat), Map.of("rank", "3"))
      ),
      (in, features) -> features.line("layer")
        .setZoomRange(13, 14)
        .inheritAttrFromSource("rank")
        .setSortKey(Integer.parseInt(in.getTag("rank").toString()))
        .setPointLabelGridSizeAndLimit(13, 128, 2)
        .setBufferPixels(128)
    );

    assertSubmap(Map.of(
      TileCoord.ofXYZ(Z14_TILES / 2, Z14_TILES / 2, 14), List.of(
        feature(newLineString(64, 64, 74, 64), Map.of("rank", "1")),
        feature(newLineString(74, 64, 84, 64), Map.of("rank", "2")),
        feature(newLineString(84, 64, 94, 64), Map.of("rank", "3"))
      ),
      TileCoord.ofXYZ(Z13_TILES / 2, Z13_TILES / 2, 13), List.of(
        // omit rank=3 due to label grid size
        feature(newLineString(32, 32, 37, 32), Map.of("rank", "1")),
        feature(newLineString(37, 32, 42, 32), Map.of("rank", "2"))
      )
    ), results.tiles);
  }

  @Test
  void testLabelGridLimitLPolygon() throws Exception {
    double y = 0.5 + Z14_WIDTH / 2;
    double lat = GeoUtils.getWorldLat(y);
    double lat2 = GeoUtils.getWorldLat(y - Z14_WIDTH * 10d / 256);

    double x1 = 0.5 + Z14_WIDTH / 4;
    double lng1 = GeoUtils.getWorldLon(x1);
    double lng2 = GeoUtils.getWorldLon(x1 + Z14_WIDTH * 10d / 256);
    double lng3 = GeoUtils.getWorldLon(x1 + Z14_WIDTH * 20d / 256);
    double lng4 = GeoUtils.getWorldLon(x1 + Z14_WIDTH * 30d / 256);

    var results = runWithReaderFeatures(
      Map.of("threads", "1"),
      List.of(
        newReaderFeature(rectangle(lng1, lat, lng2, lat2), Map.of("rank", "1")),
        newReaderFeature(rectangle(lng2, lat, lng3, lat2), Map.of("rank", "2")),
        newReaderFeature(rectangle(lng3, lat, lng4, lat2), Map.of("rank", "3"))
      ),
      (in, features) -> features.polygon("layer")
        .setZoomRange(13, 14)
        .inheritAttrFromSource("rank")
        .setSortKey(Integer.parseInt(in.getTag("rank").toString()))
        .setPointLabelGridSizeAndLimit(13, 128, 2)
        .setBufferPixels(128)
    );

    assertSubmap(Map.of(
      TileCoord.ofXYZ(Z14_TILES / 2, Z14_TILES / 2, 14), List.of(
        feature(rectangle(64, 128, 74, 118), Map.of("rank", "1")),
        feature(rectangle(74, 128, 84, 118), Map.of("rank", "2")),
        feature(rectangle(84, 128, 94, 118), Map.of("rank", "3"))
      ),
      TileCoord.ofXYZ(Z13_TILES / 2, Z13_TILES / 2, 13), List.of(
        // omit rank=3 due to label grid size
        feature(rectangle(32, 64, 37, 59), Map.of("rank", "1")),
        feature(rectangle(37, 64, 42, 59), Map.of("rank", "2"))
      )
    ), results.tiles);
  }
}
