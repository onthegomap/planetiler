package com.onthegomap.planetiler;

import static com.onthegomap.planetiler.TestUtils.*;
import static org.junit.jupiter.api.Assertions.*;

import com.onthegomap.planetiler.TestUtils.OsmXml;
import com.onthegomap.planetiler.collection.FeatureGroup;
import com.onthegomap.planetiler.collection.LongLongMap;
import com.onthegomap.planetiler.collection.LongLongMultimap;
import com.onthegomap.planetiler.config.Arguments;
import com.onthegomap.planetiler.config.MbtilesMetadata;
import com.onthegomap.planetiler.config.PlanetilerConfig;
import com.onthegomap.planetiler.geo.GeoUtils;
import com.onthegomap.planetiler.geo.GeometryException;
import com.onthegomap.planetiler.geo.TileCoord;
import com.onthegomap.planetiler.mbtiles.Mbtiles;
import com.onthegomap.planetiler.mbtiles.MbtilesWriter;
import com.onthegomap.planetiler.reader.SimpleFeature;
import com.onthegomap.planetiler.reader.SimpleReader;
import com.onthegomap.planetiler.reader.SourceFeature;
import com.onthegomap.planetiler.reader.osm.OsmBlockSource;
import com.onthegomap.planetiler.reader.osm.OsmElement;
import com.onthegomap.planetiler.reader.osm.OsmReader;
import com.onthegomap.planetiler.reader.osm.OsmRelationInfo;
import com.onthegomap.planetiler.stats.Stats;
import com.onthegomap.planetiler.worker.WorkerPipeline;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.DoubleStream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.MultiPolygon;
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
  private static final int Z14_TILES = 1 << 14;
  private static final double Z14_WIDTH = 1d / Z14_TILES;
  private static final int Z13_TILES = 1 << 13;
  private static final double Z13_WIDTH = 1d / Z13_TILES;
  private static final int Z12_TILES = 1 << 12;
  private static final int Z4_TILES = 1 << 4;
  private static final Polygon worldPolygon = newPolygon(
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


  private static <T extends OsmElement> T with(T elem, Consumer<T> fn) {
    fn.accept(elem);
    return elem;
  }

  private void processReaderFeatures(FeatureGroup featureGroup, Profile profile, PlanetilerConfig config,
    List<? extends SourceFeature> features) {
    new SimpleReader(profile, stats, "test") {

      @Override
      public long getCount() {
        return features.size();
      }

      @Override
      public WorkerPipeline.SourceStep<SourceFeature> read() {
        return features::forEach;
      }

      @Override
      public void close() {}
    }.process(featureGroup, config);
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
    FeatureGroup featureGroup = FeatureGroup.newInMemoryFeatureGroup(profile, stats);
    runner.run(featureGroup, profile, config);
    featureGroup.prepare();
    try (Mbtiles db = Mbtiles.newInMemoryDatabase(config.compactDb())) {
      MbtilesWriter.writeOutput(featureGroup, db, () -> 0L, new MbtilesMetadata(profile, config.arguments()), config,
        stats);
      var tileMap = TestUtils.getTileMap(db);
      tileMap.values().forEach(fs -> fs.forEach(f -> f.geometry().validate()));
      int tileDataCount = config.compactDb() ? TestUtils.getTilesDataCount(db) : 0;
      return new PlanetilerResults(tileMap, db.metadata().getAll(), tileDataCount);
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
    BiConsumer<SourceFeature, FeatureCollector> profileFunction
  ) throws Exception {
    return run(
      args,
      (featureGroup, profile, config) -> processOsmFeatures(featureGroup, profile, config, features),
      TestProfile.processSourceFeatures(profileFunction)
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
  void testOverrideMetadata() throws Exception {
    var results = runWithReaderFeatures(
      Map.of(
        "mbtiles_name", "mbtiles_name",
        "mbtiles_description", "mbtiles_description",
        "mbtiles_attribution", "mbtiles_attribution",
        "mbtiles_version", "mbtiles_version",
        "mbtiles_type", "mbtiles_type"
      ),
      List.of(),
      (sourceFeature, features) -> {
      }
    );
    assertEquals(Map.of(), results.tiles);
    assertSubmap(Map.of(
      "name", "mbtiles_name",
      "description", "mbtiles_description",
      "attribution", "mbtiles_attribution",
      "version", "mbtiles_version",
      "type", "mbtiles_type"
    ), results.metadata);
  }

  @Test
  void testSinglePoint() throws Exception {
    double x = 0.5 + Z14_WIDTH / 2;
    double y = 0.5 + Z14_WIDTH / 2;
    double lat = GeoUtils.getWorldLat(y);
    double lng = GeoUtils.getWorldLon(x);

    var results = runWithReaderFeatures(
      Map.of("threads", "1"),
      List.of(
        newReaderFeature(newPoint(lng, lat), Map.of(
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

  @Test
  void testLineString() throws Exception {
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

  public List<Coordinate> z14CoordinatePixelList(double... coords) {
    return z14CoordinateList(DoubleStream.of(coords).map(c -> c / 256d).toArray());
  }

  @Test
  void testPolygonWithHoleSpanningMultipleTiles() throws Exception {
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
      (in, features) -> features.polygon("layer")
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
  void testFullWorldPolygon() throws Exception {
    var results = runWithReaderFeatures(
      Map.of("threads", "1"),
      List.of(
        newReaderFeature(worldPolygon, Map.of())
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

  @ParameterizedTest
  @CsvSource({
    "chesapeake.wkb, 4076",
    "mdshore.wkb,    19904",
    "njshore.wkb,    10571"
  })
  void testComplexShorelinePolygons__TAKES_A_MINUTE_OR_TWO(String fileName, int expected)
    throws Exception {
    LOGGER.warn("Testing complex shoreline processing for " + fileName + " ...");
    MultiPolygon geometry = (MultiPolygon) new WKBReader()
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
    Geometry geom = tileContents.get(0).geometry().geom();
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
        ))
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
        throw new ExpectedException();
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
        ))
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
        ))
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
    Set<Long> nodes1 = new HashSet<>();
    Set<Long> nodes2 = new HashSet<>();
    var profile = new Profile.NullProfile() {
      @Override
      public void preprocessOsmNode(OsmElement.Node node) {
        if (node.hasTag("a", "b")) {
          nodes1.add(node.id());
        }
      }

      @Override
      public void preprocessOsmWay(OsmElement.Way way) {
        if (nodes1.contains(way.nodes().get(0))) {
          nodes2.add(way.nodes().get(0));
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

  @Test
  void testMergeLineStrings() throws Exception {
    double y = 0.5 + Z14_WIDTH / 2;
    double lat = GeoUtils.getWorldLat(y);

    double x1 = 0.5 + Z14_WIDTH / 4;
    double lng1 = GeoUtils.getWorldLon(x1);
    double lng2 = GeoUtils.getWorldLon(x1 + Z14_WIDTH * 10d / 256);
    double lng3 = GeoUtils.getWorldLon(x1 + Z14_WIDTH * 20d / 256);
    double lng4 = GeoUtils.getWorldLon(x1 + Z14_WIDTH * 30d / 256);

    var results = runWithReaderFeatures(
      Map.of("threads", "1"),
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
        .setZoomRange(13, 14)
        .setAttrWithMinzoom("z14attr", in.getTag("other"), 14)
        .inheritAttrFromSource("group"),
      (layer, zoom, items) -> FeatureMerge.mergeLineStrings(items, 0, 0, 0)
    );

    assertSubmap(sortListValues(Map.of(
      TileCoord.ofXYZ(Z14_TILES / 2, Z14_TILES / 2, 14), List.of(
        feature(newLineString(64, 128, 74, 128), Map.of("group", "1", "z14attr", "1")),
        feature(newLineString(74, 128, 84, 128), Map.of("group", "1", "z14attr", "2")),
        feature(newLineString(84, 128, 94, 128), Map.of("group", "2", "z14attr", "3"))
      ),
      TileCoord.ofXYZ(Z13_TILES / 2, Z13_TILES / 2, 13), List.of(
        // merge 32->37 and 37->42 since they have same attrs
        feature(newLineString(32, 64, 42, 64), Map.of("group", "1")),
        feature(newLineString(42, 64, 47, 64), Map.of("group", "2"))
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

  @Test
  void testMergePolygons() throws Exception {
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
      (layer, zoom, items) -> FeatureMerge.mergeNearbyPolygons(
        items,
        0,
        0,
        1,
        1
      )
    );

    assertSubmap(sortListValues(Map.of(
      TileCoord.ofXYZ(Z14_TILES / 2, Z14_TILES / 2, 14), List.of(
        feature(rectangle(10, 10, 30, 20), Map.of("group", "1")),
        feature(rectangle(10, 20.5, 20, 30), Map.of("group", "2"))
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
    LayerPostprocessFunction postprocessLayerFeatures
  ) implements Profile {

    TestProfile(
      BiConsumer<SourceFeature, FeatureCollector> processFeature,
      Function<OsmElement.Relation, List<OsmRelationInfo>> preprocessOsmRelation,
      LayerPostprocessFunction postprocessLayerFeatures
    ) {
      this(TEST_PROFILE_NAME, TEST_PROFILE_DESCRIPTION, TEST_PROFILE_ATTRIBUTION, TEST_PROFILE_VERSION, processFeature,
        preprocessOsmRelation,
        postprocessLayerFeatures);
    }

    static TestProfile processSourceFeatures(BiConsumer<SourceFeature, FeatureCollector> processFeature) {
      return new TestProfile(processFeature, (a) -> null, (a, b, c) -> c);
    }

    @Override
    public List<OsmRelationInfo> preprocessOsmRelation(
      OsmElement.Relation relation) {
      return preprocessOsmRelation.apply(relation);
    }

    @Override
    public void processFeature(SourceFeature sourceFeature, FeatureCollector features) {
      processFeature.accept(sourceFeature, features);
    }

    @Override
    public void release() {}

    @Override
    public List<VectorTile.Feature> postProcessLayerFeatures(String layer, int zoom,
      List<VectorTile.Feature> items) throws GeometryException {
      return postprocessLayerFeatures.process(layer, zoom, items);
    }
  }

  private static <T> List<T> orEmpty(List<T> in) {
    return in == null ? List.of() : in;
  }

  @Test
  void testBadRelation() throws Exception {
    // this threw an exception in OsmMultipolygon.build
    OsmXml osmInfo = TestUtils.readOsmXml("bad_spain_relation.xml");
    List<OsmElement> elements = new ArrayList<>();
    for (var node : orEmpty(osmInfo.nodes())) {
      elements.add(new OsmElement.Node(node.id(), node.lat(), node.lon()));
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

  @Test
  void testPlanetilerRunner(@TempDir Path tempDir) throws Exception {
    Path originalOsm = TestUtils.pathToResource("monaco-latest.osm.pbf");
    Path mbtiles = tempDir.resolve("output.mbtiles");
    Path tempOsm = tempDir.resolve("monaco-temp.osm.pbf");
    Files.copy(originalOsm, tempOsm);
    Planetiler.create(Arguments.fromArgs(
      "--tmpdir", tempDir.toString(),
      "--free-osm-after-read",
      // ensure we exercise the multi-threaded code
      "--write-threads=2",
      "--process-threads=2",
      "--threads=4"
    ))
      .setProfile(new Profile.NullProfile() {
        @Override
        public void processFeature(SourceFeature source, FeatureCollector features) {
          if (source.canBePolygon() && source.hasTag("building", "yes")) {
            features.polygon("building").setZoomRange(0, 14).setMinPixelSize(1);
          }
        }
      })
      .addOsmSource("osm", tempOsm)
      .addNaturalEarthSource("ne", TestUtils.pathToResource("natural_earth_vector.sqlite"))
      .addShapefileSource("shapefile", TestUtils.pathToResource("shapefile.zip"))
      .setOutput("mbtiles", mbtiles)
      .run();

    // make sure it got deleted after write
    assertFalse(Files.exists(tempOsm));

    try (Mbtiles db = Mbtiles.newReadOnlyDatabase(mbtiles)) {
      int features = 0;
      var tileMap = TestUtils.getTileMap(db);
      for (var tile : tileMap.values()) {
        for (var feature : tile) {
          feature.geometry().validate();
          features++;
        }
      }

      assertEquals(11, tileMap.size(), "num tiles");
      assertEquals(2146, features, "num buildings");
    }
  }

  private void runWithProfile(Path tempDir, Profile profile, boolean force) throws Exception {
    Planetiler.create(Arguments.of("tmpdir", tempDir, "force", Boolean.toString(force)))
      .setProfile(profile)
      .addOsmSource("osm", TestUtils.pathToResource("monaco-latest.osm.pbf"))
      .addNaturalEarthSource("ne", TestUtils.pathToResource("natural_earth_vector.sqlite"))
      .addShapefileSource("shapefile", TestUtils.pathToResource("shapefile.zip"))
      .setOutput("mbtiles", tempDir.resolve("output.mbtiles"))
      .run();
  }

  @Test
  void testPlanetilerMemoryCheck(@TempDir Path tempDir) {
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
  void testPlanetilerMemoryCheckForce(@TempDir Path tempDir) throws Exception {
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
      Map.of("threads", "1", "compact-db", Boolean.toString(compactDbEnabled)),
      List.of(
        newReaderFeature(worldPolygon, Map.of())
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
}
