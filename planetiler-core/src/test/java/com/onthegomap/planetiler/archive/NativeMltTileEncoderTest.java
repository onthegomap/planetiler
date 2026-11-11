package com.onthegomap.planetiler.archive;

import static com.onthegomap.planetiler.TestUtils.assumeNativeMlt;
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
import com.onthegomap.planetiler.util.TileSizeStats;
import java.math.BigDecimal;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.function.IntFunction;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Named;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments.ArgumentSet;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.locationtech.jts.geom.Geometry;

class NativeMltTileEncoderTest {

  @BeforeEach
  void assumeNativeLibraryLoads() {
    assumeNativeMlt();
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

  private static VectorTile tileOfManyFeatures(IntFunction<Geometry> geometry, IntFunction<Map<String, Object>> tags) {
    return new VectorTile().addLayerFeatures("layer", IntStream.range(0, 300)
      .mapToObj(i -> feature("layer", i + 1, geometry.apply(i), tags.apply(i)))
      .toList());
  }

  private static PlanetilerConfig config(String mltOptions) {
    return PlanetilerConfig.from(Arguments.fromArgs(("--tile-format=mlt " + mltOptions).split(" ")));
  }

  private static TileEncoder.Encoded encode(String mltOptions, VectorTile tile, boolean includeIds) {
    try (var encoder = new NativeMltTileEncoder(config(mltOptions), Stats.inMemory())) {
      return encoder.encode(tile, includeIds);
    }
  }

  private static List<ComparableFeature> encodeAndDecode(VectorTile tile) {
    return decodeMltWithNativeDecoder(encode("", tile, true).bytes());
  }

  /** Wound like MVT expects, with the hole opposite the shell. */
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

  @ParameterizedTest
  @MethodSource("geometries")
  void preservesGeometry(Geometry geometry) {
    var decoded = encodeAndDecode(tileWith(feature("layer", 1, geometry, Map.of())));

    assertEquals(List.of(TestUtils.feature(geometry, "layer", Map.of(), 1)), decoded);
  }

  @Test
  void preservesAttributeTypesAndStoresOtherValuesAsText() {
    Map<String, Object> tags = new HashMap<>(Map.of("string", "a", "integer", 1, "boolean", true, "float", 1.5f,
      "double", 2.25, "decimal", new BigDecimal("1.5"), "short", (short) 3));
    tags.put("missing", null);

    var decoded = encodeAndDecode(tileWith(feature("layer", 1, newPoint(1, 2), tags)));

    assertEquals(List.of(TestUtils.feature(newPoint(1, 2), "layer", Map.of("string", "a", "integer", 1L,
      "boolean", true, "float", 1.5f, "double", 2.25, "decimal", "1.5", "short", "3"), 1)), decoded);
  }

  @Test
  void widensMixedIntegersAndFloatsOfOneKeyToDoubleAndOtherMixesToText() {
    var tile = tileWith(
      feature("layer", 1, newPoint(1, 2), Map.of("width", 3, "ref", "A1")),
      feature("layer", 2, newPoint(3, 4), Map.of("width", 1.5, "ref", 12)),
      feature("layer", 3, newPoint(5, 6), Map.of("width", 2, "ref", 1.5)));

    var decoded = encodeAndDecode(tile);

    assertEquals(List.of(
      TestUtils.feature(newPoint(1, 2), "layer", Map.of("width", 3.0, "ref", "A1"), 1),
      TestUtils.feature(newPoint(3, 4), "layer", Map.of("width", 1.5, "ref", "12"), 2),
      TestUtils.feature(newPoint(5, 6), "layer", Map.of("width", 2.0, "ref", "1.5"), 3)
    ), decoded);
  }

  @Test
  void preservesLayersInNameOrderAndFeatureIds() {
    var tile = tileWith(
      feature("roads", 2, newLineString(0, 0, 10, 10), Map.of()),
      feature("pois", 1, newPoint(1, 2), Map.of()),
      feature("pois", 3, newPoint(3, 4), Map.of()));

    var decoded = encodeAndDecode(tile);

    assertEquals(List.of(
      TestUtils.feature(newPoint(1, 2), "pois", Map.of(), 1),
      TestUtils.feature(newPoint(3, 4), "pois", Map.of(), 3),
      TestUtils.feature(newLineString(0, 0, 10, 10), "roads", Map.of(), 2)
    ), decoded);
  }

  @Test
  void leavesOutIdsWhenAskedTo() {
    var tile = tileWith(feature("layer", 7, newPoint(1, 2), Map.of()));

    var decoded = decodeMltWithNativeDecoder(encode("", tile, false).bytes());

    assertEquals(List.of(TestUtils.feature(newPoint(1, 2), "layer", Map.of(), VectorTile.NO_FEATURE_ID)), decoded);
  }

  @Test
  void reusedEncoderDoesNotCarryFeaturesIntoTheNextTile() {
    try (var encoder = new NativeMltTileEncoder(PlanetilerConfig.defaults(), Stats.inMemory())) {
      encoder.encode(tileWith(feature("first", 1, newPoint(1, 2), Map.of("name", "first"))), true);

      var second = encoder.encode(tileWith(feature("second", 2, newPoint(3, 4), Map.of("ref", 2))), true);

      assertEquals(List.of(TestUtils.feature(newPoint(3, 4), "second", Map.of("ref", 2L), 2)),
        decodeMltWithNativeDecoder(second.bytes()));
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"java", "native"})
  void skipsFeaturesOfUnknownGeometryType(String mltEncoder) {
    var unknown = new VectorTile.VectorGeometry(VectorTile.encodeGeometry(newPoint(1, 2)).commands(),
      GeometryType.UNKNOWN, 0);
    var tile = new VectorTile()
      .addLayerFeatures("layer", List.of(
        new VectorTile.Feature("layer", 1, unknown, Map.of("name", "skipped")),
        feature("layer", 2, newPoint(3, 4), Map.of("name", "kept"))))
      .addLayerFeatures("onlyUnknown", List.of(new VectorTile.Feature("onlyUnknown", 3, unknown, Map.of())));
    var stats = Stats.inMemory();

    TileEncoder.Encoded encoded;
    try (var encoder = TileEncoder.factory(config("--mlt-encoder=" + mltEncoder), stats).get()) {
      encoded = encoder.encode(tile, true);
    }

    assertEquals(List.of(TestUtils.feature(newPoint(3, 4), "layer", Map.of("name", "kept"), 2)),
      decodeMltWithNativeDecoder(encoded.bytes()));
    assertEquals(List.of("layer"), encoded.layerStats().stream().map(TileSizeStats.LayerStats::layer).toList());
    assertEquals(Map.of("mlt_feature", 2L), stats.dataErrors());
  }

  @ParameterizedTest
  @ValueSource(strings = {"", "--mlt-fastpfor --mlt-fsst --mlt-shared-dict"})
  void returnsStatsOfEachLayer(String mltOptions) {
    // shared dictionaries group name:en and name:de into a nested column
    var tile = tileWith(
      feature("pois", 1, newPoint(1, 2), Map.of("name:en", "a", "name:de", "b")),
      feature("roads", 2, newMultiLineString(newLineString(0, 0, 5, 5), newLineString(7, 7, 9, 12)), Map.of()));

    var stats = encode(mltOptions, tile, true).layerStats();

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

  @ParameterizedTest
  @ValueSource(strings = {
    "--mlt-advanced",
    "--mlt-reorder-features",
    "--mlt-shared-dict",
    "--mlt-tessellate-polygons",
    "--mlt-fastpfor --mlt-fsst --mlt-reorder-features --mlt-shared-dict --mlt-tessellate-polygons"
  })
  void mltOptionsDoNotChangeTheDecodedFeatures(String mltOptions) {
    var tile = tileWith(
      feature("pois", 1, newPoint(1, 2), Map.of("name", "a", "rank", 1)),
      feature("pois", 2, newPoint(3, 4), Map.of("name", "b", "rank", 2)),
      feature("roads", 3, newLineString(0, 0, 10, 10), Map.of("oneway", true)),
      feature("water", 4, squareWithSquareHole(), Map.of("area", 1.5)));

    var decoded = decodeMltWithNativeDecoder(encode(mltOptions, tile, true).bytes());

    // --mlt-reorder-features may change the order of features within a layer
    assertEquals(new HashSet<>(List.of(
      TestUtils.feature(newPoint(1, 2), "pois", Map.of("name", "a", "rank", 1L), 1),
      TestUtils.feature(newPoint(3, 4), "pois", Map.of("name", "b", "rank", 2L), 2),
      TestUtils.feature(newLineString(0, 0, 10, 10), "roads", Map.of("oneway", true), 3),
      TestUtils.feature(squareWithSquareHole(), "water", Map.of("area", 1.5), 4)
    )), new HashSet<>(decoded));
  }

  private static Stream<ArgumentSet> optionsAndTilesTheyChange() {
    return Stream.of(
      argumentSet("--mlt-fastpfor", "--mlt-fastpfor",
        tileOfManyFeatures(i -> newPoint(i * 37 % 250, i * 91 % 250), i -> Map.of("population", i * 7919L % 100_000))),
      argumentSet("--mlt-fsst", "--mlt-fsst",
        tileOfManyFeatures(i -> newPoint(i * 37 % 250, i * 91 % 250), i -> Map.of("name", "Rue numero " + i))),
      argumentSet("--mlt-tessellate-polygons", "--mlt-tessellate-polygons",
        tileOfManyFeatures(i -> rectangle(i % 200, i % 200 + 3), i -> Map.of())));
  }

  @ParameterizedTest
  @MethodSource("optionsAndTilesTheyChange")
  void mltOptionsReachTheEncoder(String mltOptions, VectorTile tile) {
    assertNotEquals(encode("", tile, true).bytes().length, encode(mltOptions, tile, true).bytes().length);
  }
}
