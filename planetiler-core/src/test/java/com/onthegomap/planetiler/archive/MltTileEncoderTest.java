package com.onthegomap.planetiler.archive;

import static com.onthegomap.planetiler.TestUtils.assumeNativeMlt;
import static com.onthegomap.planetiler.TestUtils.decodeMltWithJavaDecoder;
import static com.onthegomap.planetiler.TestUtils.decodeMltWithNativeDecoder;
import static com.onthegomap.planetiler.TestUtils.newLineString;
import static com.onthegomap.planetiler.TestUtils.newMultiLineString;
import static com.onthegomap.planetiler.TestUtils.newMultiPoint;
import static com.onthegomap.planetiler.TestUtils.newMultiPolygon;
import static com.onthegomap.planetiler.TestUtils.newPoint;
import static com.onthegomap.planetiler.TestUtils.newPolygon;
import static com.onthegomap.planetiler.TestUtils.rectangle;
import static com.onthegomap.planetiler.TestUtils.rectangleCoordList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.params.provider.Arguments.argumentSet;

import com.onthegomap.planetiler.TestUtils;
import com.onthegomap.planetiler.TestUtils.ComparableFeature;
import com.onthegomap.planetiler.VectorTile;
import com.onthegomap.planetiler.config.Arguments;
import com.onthegomap.planetiler.config.PlanetilerConfig;
import com.onthegomap.planetiler.geo.GeometryType;
import com.onthegomap.planetiler.stats.Stats;
import com.onthegomap.planetiler.util.Gzip;
import com.onthegomap.planetiler.util.TileSizeStats;
import java.math.BigDecimal;
import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.function.IntFunction;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Named;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments.ArgumentSet;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.locationtech.jts.geom.Geometry;

class MltTileEncoderTest {

  /** The MLT encoders, each checked with its reference decoder. */
  enum Encoder {
    NATIVE {
      @Override
      void assumeAvailable() {
        assumeNativeMlt();
      }

      @Override
      TileEncoder newEncoder(PlanetilerConfig config, Stats stats) {
        return new NativeMltTileEncoder(config, stats);
      }

      @Override
      List<ComparableFeature> decode(byte[] mlt) {
        return decodeMltWithNativeDecoder(mlt);
      }
    },
    JAVA {
      @Override
      TileEncoder newEncoder(PlanetilerConfig config, Stats stats) {
        return new JavaMltTileEncoder(config, stats);
      }

      @Override
      List<ComparableFeature> decode(byte[] mlt) {
        return decodeMltWithJavaDecoder(mlt);
      }
    };

    /** Skips the calling test if this encoder cannot run on this platform. */
    void assumeAvailable() {
      // the Java encoder runs on every platform, so only the native encoder overrides this
    }

    abstract TileEncoder newEncoder(PlanetilerConfig config, Stats stats);

    abstract List<ComparableFeature> decode(byte[] mlt);

    TileEncoder create(PlanetilerConfig config) {
      return create(config, Stats.inMemory());
    }

    TileEncoder create(PlanetilerConfig config, Stats stats) {
      assumeAvailable();
      return newEncoder(config, stats);
    }
  }

  private static VectorTile.Feature feature(String layer, long id, Geometry geometry, Map<String, Object> tags) {
    return new VectorTile.Feature(layer, id, VectorTile.encodeGeometry(geometry), tags);
  }

  private static VectorTile tileWith(VectorTile.Feature... features) {
    var tile = new VectorTile();
    for (var feature : features) {
      tile.addLayerFeatures(feature.layer(), List.of(feature));
    }
    return tile;
  }

  private static final VectorTile POINTS_WITH_INTEGERS = tileOfManyFeatures(
    i -> newPoint(i * 37 % 250, i * 91 % 250), i -> Map.of("population", (long) (i * 7919 % 100_000)));
  private static final VectorTile POINTS_WITH_UNIQUE_STRINGS = tileOfManyFeatures(
    i -> newPoint(i * 37 % 250, i * 91 % 250), i -> Map.of("name", "Rue de la Republique numero " + i));
  private static final VectorTile POINTS_WITH_SHUFFLED_IDS = new VectorTile().addLayerFeatures("layer",
    IntStream.range(0, 300).mapToObj(i -> feature("layer", i * 7919 % 100_003, newPoint(i % 17 * 14, i / 17 * 14),
      Map.of())).toList());
  private static final VectorTile SQUARES = tileOfManyFeatures(i -> rectangle(i % 200, i % 200 + 3), i -> Map.of());

  private static VectorTile tileOfManyFeatures(IntFunction<Geometry> geometry, IntFunction<Map<String, Object>> tags) {
    var features = IntStream.range(0, 300)
      .mapToObj(i -> feature("layer", i + 1, geometry.apply(i), tags.apply(i)))
      .toList();
    return new VectorTile().addLayerFeatures("layer", features);
  }

  private static int encodedSize(Encoder encoder, String mltOptions, VectorTile tile) {
    var config = PlanetilerConfig.from(Arguments.fromArgs(("--tile-format=mlt " + mltOptions).split(" ")));
    try (var mltEncoder = encoder.create(config)) {
      return mltEncoder.encode(tile, true).bytes().length;
    }
  }

  private static List<ComparableFeature> encodeAndDecode(Encoder encoder, PlanetilerConfig config, VectorTile tile,
    boolean includeIds) {
    try (var mltEncoder = encoder.create(config)) {
      return encoder.decode(mltEncoder.encode(tile, includeIds).bytes());
    }
  }

  private static List<ComparableFeature> encodeAndDecode(Encoder encoder, VectorTile tile) {
    return encodeAndDecode(encoder, PlanetilerConfig.defaults(), tile, true);
  }

  /** Returns a square with a square hole, wound like MVT expects: the hole in the opposite direction of the shell. */
  private static Geometry squareWithSquareHole() {
    return newPolygon(rectangleCoordList(0, 100), List.of(rectangleCoordList(20, 40))).norm().reverse();
  }

  private static Stream<Named<Geometry>> geometries() {
    return Stream.of(
      Named.of("point", newPoint(1, 2)),
      Named.of("multipoint", newMultiPoint(newPoint(3, 4), newPoint(5, 6))),
      Named.of("line", newLineString(0, 0, 10, 10)),
      Named.of("multiline", newMultiLineString(newLineString(0, 0, 5, 5), newLineString(7, 7, 9, 12))),
      Named.of("polygon", rectangle(10, 20)),
      Named.of("polygon with hole", squareWithSquareHole()),
      Named.of("multipolygon", newMultiPolygon(rectangle(0, 10), rectangle(20, 30)))
    );
  }

  private static Stream<ArgumentSet> encodersAndGeometries() {
    return Stream.of(Encoder.values()).flatMap(encoder -> geometries()
      .map(geometry -> argumentSet(encoder + " " + geometry.getName(), encoder, geometry.getPayload())));
  }

  @ParameterizedTest
  @MethodSource("encodersAndGeometries")
  void preservesGeometry(Encoder encoder, Geometry geometry) {
    var tile = tileWith(feature("layer", 1, geometry, Map.of()));

    var decoded = encodeAndDecode(encoder, tile);

    assertEquals(List.of(TestUtils.feature(geometry, "layer", Map.of(), 1)), decoded);
  }

  @ParameterizedTest
  @EnumSource(Encoder.class)
  void preservesAttributesOfEveryType(Encoder encoder) {
    var tile = tileWith(feature("layer", 1, newPoint(1, 2),
      Map.of("string", "a", "integer", 1, "boolean", true, "float", 1.5f, "double", 2.25)));

    var decoded = encodeAndDecode(encoder, tile);

    assertEquals(List.of(TestUtils.feature(newPoint(1, 2), "layer",
      Map.of("string", "a", "integer", 1L, "boolean", true, "float", 1.5f, "double", 2.25), 1)), decoded);
  }

  @ParameterizedTest
  @EnumSource(Encoder.class)
  void skipsAttributesWithoutValue(Encoder encoder) {
    Map<String, Object> tags = new HashMap<>();
    tags.put("missing", null);
    tags.put("present", "a");
    var tile = tileWith(feature("layer", 1, newPoint(1, 2), tags));

    var decoded = encodeAndDecode(encoder, tile);

    assertEquals(List.of(TestUtils.feature(newPoint(1, 2), "layer", Map.of("present", "a"), 1)), decoded);
  }

  @ParameterizedTest
  @EnumSource(Encoder.class)
  void leavesOutLayersWithoutFeatures(Encoder encoder) {
    var noGeometry = new VectorTile.VectorGeometry(new int[0], GeometryType.POINT, 0);
    var tile = tileWith(
      new VectorTile.Feature("empty", 1, noGeometry, Map.of()),
      feature("layer", 2, newPoint(1, 2), Map.of()));

    var decoded = encodeAndDecode(encoder, tile);

    assertEquals(List.of(TestUtils.feature(newPoint(1, 2), "layer", Map.of(), 2)), decoded);
  }

  @ParameterizedTest
  @EnumSource(Encoder.class)
  void preservesLayersInNameOrderAndFeatureIds(Encoder encoder) {
    var tile = tileWith(
      feature("roads", 2, newLineString(0, 0, 10, 10), Map.of()),
      feature("pois", 1, newPoint(1, 2), Map.of()),
      feature("pois", 3, newPoint(3, 4), Map.of()));

    var decoded = encodeAndDecode(encoder, tile);

    assertEquals(List.of(
      TestUtils.feature(newPoint(1, 2), "pois", Map.of(), 1),
      TestUtils.feature(newPoint(3, 4), "pois", Map.of(), 3),
      TestUtils.feature(newLineString(0, 0, 10, 10), "roads", Map.of(), 2)
    ), decoded);
  }

  @ParameterizedTest
  @EnumSource(Encoder.class)
  void leavesOutIdsWhenAskedTo(Encoder encoder) {
    var tile = tileWith(feature("layer", 7, newPoint(1, 2), Map.of()));

    var decoded = encodeAndDecode(encoder, PlanetilerConfig.defaults(), tile, false);

    assertEquals(List.of(TestUtils.feature(newPoint(1, 2), "layer", Map.of(), VectorTile.NO_FEATURE_ID)), decoded);
  }

  @ParameterizedTest
  @EnumSource(Encoder.class)
  void mixedTextAndIntegerValuesOfOneKeyBecomeText(Encoder encoder) {
    var tile = tileWith(
      feature("layer", 1, newPoint(1, 2), Map.of("ref", "A1")),
      feature("layer", 2, newPoint(3, 4), Map.of("ref", 12)));

    var decoded = encodeAndDecode(encoder, tile);

    assertEquals(List.of(
      TestUtils.feature(newPoint(1, 2), "layer", Map.of("ref", "A1"), 1),
      TestUtils.feature(newPoint(3, 4), "layer", Map.of("ref", "12"), 2)
    ), decoded);
  }

  @ParameterizedTest
  @EnumSource(Encoder.class)
  void mixedTextIntegerAndFloatValuesOfOneKeyBecomeText(Encoder encoder) {
    var tile = tileWith(
      feature("layer", 1, newPoint(1, 2), Map.of("ref", "A1")),
      feature("layer", 2, newPoint(3, 4), Map.of("ref", 12)),
      feature("layer", 3, newPoint(5, 6), Map.of("ref", 1.5)));

    var decoded = encodeAndDecode(encoder, tile);

    assertEquals(List.of(
      TestUtils.feature(newPoint(1, 2), "layer", Map.of("ref", "A1"), 1),
      TestUtils.feature(newPoint(3, 4), "layer", Map.of("ref", "12"), 2),
      TestUtils.feature(newPoint(5, 6), "layer", Map.of("ref", "1.5"), 3)
    ), decoded);
  }

  @ParameterizedTest
  @EnumSource(Encoder.class)
  void reusedEncoderDoesNotCarryFeaturesIntoTheNextTile(Encoder encoder) {
    try (var mltEncoder = encoder.create(PlanetilerConfig.defaults())) {
      mltEncoder.encode(tileWith(feature("first", 1, newPoint(1, 2), Map.of("name", "first"))), true);

      var second = encoder
        .decode(mltEncoder.encode(tileWith(feature("second", 2, newPoint(3, 4), Map.of("ref", 2))), true).bytes());

      assertEquals(List.of(TestUtils.feature(newPoint(3, 4), "second", Map.of("ref", 2L), 2)), second);
    }
  }

  private static Stream<ArgumentSet> encodersAndStatsOptions() {
    return Stream.of(Encoder.values()).flatMap(encoder -> Stream.of("", "--mlt-fastpfor --mlt-fsst --mlt-shared-dict")
      .map(options -> argumentSet(encoder + " " + options, encoder, options)));
  }

  @ParameterizedTest
  @MethodSource("encodersAndStatsOptions")
  void returnsStatsOfEachLayer(Encoder encoder, String mltOptions) {
    var config = PlanetilerConfig.from(Arguments.fromArgs(("--tile-format=mlt " + mltOptions).split(" ")));
    // shared dictionaries group name:en and name:de into a nested column
    var tile = tileWith(
      feature("pois", 1, newPoint(1, 2), Map.of("name:en", "a", "name:de", "b")),
      feature("roads", 2, newMultiLineString(newLineString(0, 0, 5, 5), newLineString(7, 7, 9, 12)), Map.of()));

    List<TileSizeStats.LayerStats> stats;
    try (var mltEncoder = encoder.create(config)) {
      stats = mltEncoder.encode(tile, true).layerStats();
    }

    assertEquals(List.of("pois", "roads"), stats.stream().map(TileSizeStats.LayerStats::layer).toList());
    var pois = stats.getFirst();
    assertEquals(List.of(1, 1, 2, 2), List.of(pois.layerFeatures(), pois.layerGeometries(), pois.layerAttrKeys(),
      pois.layerAttrValues()));
    assertTrue(pois.layerBytes() > pois.layerAttrBytes() && pois.layerAttrBytes() > 0, pois.toString());
    var roads = stats.get(1);
    assertEquals(List.of(1, 2, 0, 0), List.of(roads.layerFeatures(), roads.layerGeometries(), roads.layerAttrKeys(),
      roads.layerAttrValues()));
    assertTrue(roads.layerBytes() > 0, roads.toString());
  }

  private static Stream<ArgumentSet> encodersAndOptions() {
    return Stream.of(Encoder.values()).flatMap(encoder -> Stream.of(
      "--mlt-advanced",
      "--mlt-fsst",
      "--mlt-fastpfor",
      "--mlt-reorder-features",
      "--mlt-shared-dict",
      "--mlt-tessellate-polygons --mlt-polygon-outline",
      "--mlt-fastpfor --mlt-fsst --mlt-reorder-features --mlt-shared-dict --mlt-tessellate-polygons --mlt-polygon-outline"
    ).map(options -> argumentSet(encoder + " " + options, encoder, options)));
  }

  @ParameterizedTest
  @MethodSource("encodersAndOptions")
  void mltOptionsDoNotChangeTheDecodedFeatures(Encoder encoder, String mltOptions) {
    var config = PlanetilerConfig.from(Arguments.fromArgs(("--tile-format=mlt " + mltOptions).split(" ")));
    var tile = tileWith(
      feature("pois", 1, newPoint(1, 2), Map.of("name", "a", "rank", 1)),
      feature("pois", 2, newPoint(3, 4), Map.of("name", "b", "rank", 2)),
      feature("roads", 3, newLineString(0, 0, 10, 10), Map.of("oneway", true)),
      feature("water", 4, squareWithSquareHole(), Map.of("area", 1.5)));

    var decoded = encodeAndDecode(encoder, config, tile, true);

    // --mlt-reorder-features may change the order of features within a layer
    assertEquals(new HashSet<>(List.of(
      TestUtils.feature(newPoint(1, 2), "pois", Map.of("name", "a", "rank", 1L), 1),
      TestUtils.feature(newPoint(3, 4), "pois", Map.of("name", "b", "rank", 2L), 2),
      TestUtils.feature(newLineString(0, 0, 10, 10), "roads", Map.of("oneway", true), 3),
      TestUtils.feature(squareWithSquareHole(), "water", Map.of("area", 1.5), 4)
    )), new HashSet<>(decoded));
  }

  @ParameterizedTest
  @EnumSource(Encoder.class)
  void fastPforShrinksIntegers(Encoder encoder) {
    assertTrue(encodedSize(encoder, "--mlt-fastpfor", POINTS_WITH_INTEGERS) <
      encodedSize(encoder, "", POINTS_WITH_INTEGERS));
  }

  @ParameterizedTest
  @EnumSource(Encoder.class)
  void fsstLeavesTilesWithoutStringsUnchanged(Encoder encoder) {
    // fails if mlt_fsst turns on FastPFOR, which shrinks this tile
    assertEquals(encodedSize(encoder, "", POINTS_WITH_INTEGERS),
      encodedSize(encoder, "--mlt-fsst", POINTS_WITH_INTEGERS));
  }

  @ParameterizedTest
  @EnumSource(Encoder.class)
  void tessellationAddsTrianglesToPolygons(Encoder encoder) {
    assertTrue(encodedSize(encoder, "--mlt-tessellate-polygons --mlt-polygon-outline", SQUARES) >
      encodedSize(encoder, "", SQUARES));
  }

  @ParameterizedTest
  @EnumSource(Encoder.class)
  void fsstShrinksUniqueStrings(Encoder encoder) {
    assertTrue(encodedSize(encoder, "--mlt-fsst", POINTS_WITH_UNIQUE_STRINGS) <
      encodedSize(encoder, "", POINTS_WITH_UNIQUE_STRINGS));
  }

  // the native encoder only keeps a sort that shrinks the tile, which no small tile tried in testing did
  @Test
  void javaEncoderReorderFeaturesChangesTheEncoding() {
    assertNotEquals(encodedSize(Encoder.JAVA, "", POINTS_WITH_SHUFFLED_IDS),
      encodedSize(Encoder.JAVA, "--mlt-reorder-features", POINTS_WITH_SHUFFLED_IDS));
  }

  @Test
  void nativeDecoderReadsTilesOfTheJavaEncoder() {
    assumeNativeMlt();
    var tile = tileWith(
      feature("pois", 1, newPoint(1, 2), Map.of("name", "a", "rank", 1, "area", 1.5, "open", true)),
      feature("water", 2, squareWithSquareHole(), Map.of()));

    byte[] mlt;
    try (var encoder = Encoder.JAVA.create(PlanetilerConfig.defaults())) {
      mlt = encoder.encode(tile, true).bytes();
    }

    assertEquals(decodeMltWithJavaDecoder(mlt), decodeMltWithNativeDecoder(mlt));
  }

  // only the native encoder holds per-instance state that threads could share by mistake
  @Test
  void nativeEncodersOnSeparateThreadsDoNotInterfere() throws Exception {
    var encoder = Encoder.NATIVE;
    encoder.assumeAvailable();
    int threads = 8;
    try (var pool = Executors.newFixedThreadPool(threads)) {
      List<Callable<List<ComparableFeature>>> tasks = new ArrayList<>();
      for (int thread = 0; thread < threads; thread++) {
        int n = thread + 1;
        long id = n;
        tasks.add(() -> {
          var tile = tileWith(feature("layer", id, newPoint(n, n), Map.of("thread", id)));
          List<ComparableFeature> decoded = List.of();
          try (var threadEncoder = encoder.create(PlanetilerConfig.defaults())) {
            for (int i = 0; i < 200; i++) {
              decoded = encoder.decode(threadEncoder.encode(tile, true).bytes());
            }
          }
          return decoded;
        });
      }

      var results = pool.invokeAll(tasks);

      for (int thread = 0; thread < threads; thread++) {
        int n = thread + 1;
        long id = n;
        assertEquals(List.of(TestUtils.feature(newPoint(n, n), "layer", Map.of("thread", id), id)),
          results.get(thread).get());
      }
    }
  }

  /** Opt-in check against real tiles: {@code -Dmlt.test.mbtiles=path/to/file.mbtiles}. */
  @ParameterizedTest
  @EnumSource(Encoder.class)
  void everyTileOfAnMbtilesFileDecodesToTheSameFeaturesAsMvt(Encoder encoder) throws Exception {
    String mbtiles = System.getProperty("mlt.test.mbtiles");
    Assumptions.assumeTrue(mbtiles != null, "set -Dmlt.test.mbtiles to run");
    try (var mltEncoder = encoder.create(PlanetilerConfig.defaults());
      var connection = DriverManager.getConnection("jdbc:sqlite:" + mbtiles);
      var rs = connection.createStatement().executeQuery("select tile_data from tiles")) {
      while (rs.next()) {
        var tile = new VectorTile();
        VectorTile.decode(Gzip.gunzip(rs.getBytes(1))).stream()
          .collect(Collectors.groupingBy(VectorTile.Feature::layer))
          .forEach(tile::addLayerFeatures);
        var decodedFromMvt = TestUtils.decodeMvt(tile.encode());

        var decodedFromMlt = encoder.decode(mltEncoder.encode(tile, true).bytes());

        assertEquals(decodedFromMvt, decodedFromMlt);
      }
    }
  }

  @ParameterizedTest
  @EnumSource(Encoder.class)
  void skipsFeaturesOfUnknownGeometryType(Encoder encoder) {
    var unknown = new VectorTile.VectorGeometry(VectorTile.encodeGeometry(newPoint(1, 2)).commands(),
      GeometryType.UNKNOWN, 0);
    var tile = new VectorTile()
      .addLayerFeatures("layer", List.of(
        new VectorTile.Feature("layer", 1, unknown, Map.of("name", "skipped")),
        feature("layer", 2, newPoint(3, 4), Map.of("name", "kept"))))
      .addLayerFeatures("onlyUnknown", List.of(new VectorTile.Feature("onlyUnknown", 3, unknown, Map.of())));

    var stats = Stats.inMemory();
    List<ComparableFeature> decoded;
    List<TileSizeStats.LayerStats> layerStats;
    try (var mltEncoder = encoder.create(PlanetilerConfig.defaults(), stats)) {
      var encoded = mltEncoder.encode(tile, true);
      decoded = encoder.decode(encoded.bytes());
      layerStats = encoded.layerStats();
    }

    assertEquals(List.of(TestUtils.feature(newPoint(3, 4), "layer", Map.of("name", "kept"), 2)), decoded);
    assertEquals(List.of("layer"), layerStats.stream().map(TileSizeStats.LayerStats::layer).toList());
    assertEquals(Map.of("mlt_feature", 2L), stats.dataErrors());
  }

  @Test
  void nativeEncoderWidensMixedIntegerAndFloatValuesOfOneKeyToDouble() {
    var tile = tileWith(
      feature("layer", 1, newPoint(1, 2), Map.of("width", 3)),
      feature("layer", 2, newPoint(3, 4), Map.of("width", 1.5)));

    var decoded = encodeAndDecode(Encoder.NATIVE, tile);

    assertEquals(List.of(
      TestUtils.feature(newPoint(1, 2), "layer", Map.of("width", 3.0), 1),
      TestUtils.feature(newPoint(3, 4), "layer", Map.of("width", 1.5), 2)
    ), decoded);
  }

  @ParameterizedTest
  @EnumSource(Encoder.class)
  void storesValuesOfOtherTypesAsTextLikeMvt(Encoder encoder) {
    var tile = tileWith(feature("layer", 1, newPoint(1, 2),
      Map.of("decimal", new BigDecimal("1.5"), "short", (short) 3)));

    var decoded = encodeAndDecode(encoder, tile);

    assertEquals(List.of(TestUtils.feature(newPoint(1, 2), "layer", Map.of("decimal", "1.5", "short", "3"), 1)),
      decoded);
  }

  @Test
  void javaEncoderTurnsMixedIntegerAndFloatValuesOfOneKeyIntoText() {
    var tile = tileWith(
      feature("layer", 1, newPoint(1, 2), Map.of("width", 3)),
      feature("layer", 2, newPoint(3, 4), Map.of("width", 1.5)));

    var decoded = encodeAndDecode(Encoder.JAVA, tile);

    assertEquals(List.of(
      TestUtils.feature(newPoint(1, 2), "layer", Map.of("width", "3"), 1),
      TestUtils.feature(newPoint(3, 4), "layer", Map.of("width", "1.5"), 2)
    ), decoded);
  }
}
