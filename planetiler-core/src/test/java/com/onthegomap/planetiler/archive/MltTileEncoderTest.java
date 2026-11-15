package com.onthegomap.planetiler.archive;

import static com.onthegomap.planetiler.TestUtils.decodeMlt;
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

import com.onthegomap.planetiler.TestUtils;
import com.onthegomap.planetiler.VectorTile;
import com.onthegomap.planetiler.config.Arguments;
import com.onthegomap.planetiler.config.PlanetilerConfig;
import com.onthegomap.planetiler.stats.Stats;
import java.math.BigDecimal;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.junit.jupiter.api.Named;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.locationtech.jts.geom.Geometry;

/**
 * The contract that every MLT {@link TileEncoder} meets, run against each of them by a subclass. Tiles are read back
 * with {@link TestUtils#decodeMlt}, which returns integers as longs.
 */
abstract class MltTileEncoderTest {

  protected final Stats stats = Stats.inMemory();

  abstract TileEncoder encoder(PlanetilerConfig config, Stats stats);

  /** Encodes {@code tile} with feature IDs and the given {@code --mlt-*} command-line options. */
  protected TileEncoder.Encoded encode(VectorTile tile, String... mltOptions) {
    var args = Stream.concat(Stream.of("--tile-format=mlt"), Stream.of(mltOptions)).toArray(String[]::new);
    try (var encoder = encoder(PlanetilerConfig.from(Arguments.fromArgs(args)), stats)) {
      return encoder.encode(tile, true);
    }
  }

  protected static VectorTile.Feature feature(String layer, long id, Geometry geometry, Map<String, Object> tags) {
    return new VectorTile.Feature(layer, id, VectorTile.encodeGeometry(geometry), tags);
  }

  /** Returns a tile holding {@code features}, each in the layer it names. */
  protected static VectorTile tileWith(VectorTile.Feature... features) {
    var tile = new VectorTile();
    for (var feature : features) {
      tile.addLayerFeatures(feature.layer(), List.of(feature));
    }
    return tile;
  }

  /** A square with a square hole, wound like MVT expects: the hole opposite the shell. */
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
    var tile = tileWith(feature("layer", 1, geometry, Map.of()));

    var decoded = decodeMlt(encode(tile).bytes());

    assertEquals(List.of(TestUtils.feature(geometry, "layer", Map.of(), 1)), decoded);
  }

  @Test
  void preservesAttributeTypesAndStoresOtherValuesAsText() {
    Map<String, Object> tags = new HashMap<>(Map.of(
      "string", "a",
      "integer", 1,
      "boolean", true,
      "float", 1.5f,
      "double", 2.25,
      "decimal", new BigDecimal("1.5"),
      "short", (short) 3));
    tags.put("missing", null);
    var tile = tileWith(feature("layer", 1, newPoint(1, 2), tags));

    var decoded = decodeMlt(encode(tile).bytes());

    // vector tiles have no decimal or short type, and an attribute without a value is left out
    assertEquals(List.of(TestUtils.feature(newPoint(1, 2), "layer", Map.of(
      "string", "a",
      "integer", 1L,
      "boolean", true,
      "float", 1.5f,
      "double", 2.25,
      "decimal", "1.5",
      "short", "3"), 1)), decoded);
  }

  @Test
  void writesNumberAndStringValuesOfOneKeyAsText() {
    var tile = tileWith(
      feature("building", 1, newPoint(10, 20), Map.of("height", 12)),
      feature("building", 2, newPoint(30, 40), Map.of("height", "tall")));

    var decoded = decodeMlt(encode(tile).bytes());

    assertEquals(List.of(
      TestUtils.feature(newPoint(10, 20), "building", Map.of("height", "12"), 1),
      TestUtils.feature(newPoint(30, 40), "building", Map.of("height", "tall"), 2)
    ), decoded);
  }

  @Test
  void writesNumberAndStringValuesOfOneKeyAsTextWithSharedDictionaries() {
    // 100 repeated strings in another layer are enough for shared dictionaries to kick in
    var cafes = IntStream.rangeClosed(1, 100)
      .mapToObj(id -> feature("poi", id, newPoint(id, id), Map.of("name", "cafe")))
      .toList();
    var tile = new VectorTile()
      .addLayerFeatures("poi", cafes)
      .addLayerFeatures("building", List.of(
        feature("building", 101, newPoint(10, 20), Map.of("height", 12)),
        feature("building", 102, newPoint(30, 40), Map.of("height", "tall"))));

    var decoded = decodeMlt(encode(tile, "--mlt-shared-dict").bytes());

    assertEquals(List.of(
      TestUtils.feature(newPoint(10, 20), "building", Map.of("height", "12"), 101),
      TestUtils.feature(newPoint(30, 40), "building", Map.of("height", "tall"), 102)
    ), decoded.stream().filter(feature -> feature.layer().equals("building")).toList());
    assertEquals(100, decoded.stream().filter(feature -> feature.attrs().equals(Map.of("name", "cafe"))).count());
  }

  @Test
  void writesLayersInNameOrderAndKeepsFeatureIds() {
    var tile = tileWith(
      feature("roads", 2, newLineString(0, 0, 10, 10), Map.of()),
      feature("pois", 1, newPoint(1, 2), Map.of()),
      feature("pois", 3, newPoint(3, 4), Map.of()));

    var decoded = decodeMlt(encode(tile).bytes());

    assertEquals(List.of(
      TestUtils.feature(newPoint(1, 2), "pois", Map.of(), 1),
      TestUtils.feature(newPoint(3, 4), "pois", Map.of(), 3),
      TestUtils.feature(newLineString(0, 0, 10, 10), "roads", Map.of(), 2)
    ), decoded);
  }

  @Test
  void leavesOutIdsWhenAskedTo() {
    var tile = tileWith(feature("layer", 7, newPoint(1, 2), Map.of()));

    byte[] bytes;
    try (var encoder = encoder(PlanetilerConfig.from(Arguments.of("tile_format", "mlt")), stats)) {
      bytes = encoder.encode(tile, false).bytes();
    }

    assertEquals(List.of(TestUtils.feature(newPoint(1, 2), "layer", Map.of(), VectorTile.NO_FEATURE_ID)),
      decodeMlt(bytes));
  }

  @Test
  void reusedEncoderDoesNotCarryFeaturesIntoTheNextTile() {
    try (var encoder = encoder(PlanetilerConfig.from(Arguments.of("tile_format", "mlt")), stats)) {
      encoder.encode(tileWith(feature("first", 1, newPoint(1, 2), Map.of("name", "first"))), true);

      var second = encoder.encode(tileWith(feature("second", 2, newPoint(3, 4), Map.of("ref", 2))), true);

      assertEquals(List.of(TestUtils.feature(newPoint(3, 4), "second", Map.of("ref", 2L), 2)),
        decodeMlt(second.bytes()));
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"--mlt-shared-dict=false", "--mlt-shared-dict"})
  void returnsStatsOfEachLayer(String sharedDictOption) {
    // with shared dictionaries, name:en and name:de become one nested column
    var tile = tileWith(
      feature("pois", 1, newPoint(1, 2), Map.of("name:en", "a", "name:de", "b")),
      feature("roads", 2, newMultiLineString(newLineString(0, 0, 5, 5), newLineString(7, 7, 9, 12)), Map.of()));

    var layerStats = encode(tile, sharedDictOption).layerStats();

    assertEquals(2, layerStats.size());
    var pois = layerStats.get(0);
    assertEquals("pois", pois.layer());
    assertEquals(1, pois.layerFeatures());
    assertEquals(1, pois.layerGeometries());
    assertEquals(2, pois.layerAttrKeys());
    assertEquals(2, pois.layerAttrValues());
    assertTrue(pois.layerAttrBytes() > 0, pois.toString());
    assertTrue(pois.layerBytes() > pois.layerAttrBytes(), pois.toString());
    var roads = layerStats.get(1);
    assertEquals("roads", roads.layer());
    assertEquals(1, roads.layerFeatures());
    assertEquals(2, roads.layerGeometries());
    assertEquals(0, roads.layerAttrKeys());
    assertEquals(0, roads.layerAttrValues());
    assertTrue(roads.layerBytes() > 0, roads.toString());
  }

  @ParameterizedTest
  @ValueSource(strings = {
    "--mlt-advanced",
    "--mlt-reorder-features",
    "--mlt-shared-dict",
    "--mlt-tessellate-polygons --mlt-polygon-outline",
    "--mlt-fastpfor --mlt-fsst --mlt-reorder-features --mlt-shared-dict --mlt-tessellate-polygons --mlt-polygon-outline"
  })
  void mltOptionsDoNotChangeTheDecodedFeatures(String mltOptions) {
    var tile = tileWith(
      feature("pois", 1, newPoint(1, 2), Map.of("name", "a", "rank", 1)),
      feature("pois", 2, newPoint(3, 4), Map.of("name", "b", "rank", 2)),
      feature("roads", 3, newLineString(0, 0, 10, 10), Map.of("oneway", true)),
      feature("water", 4, squareWithSquareHole(), Map.of("area", 1.5)));

    var decoded = decodeMlt(encode(tile, mltOptions.split(" ")).bytes());

    // --mlt-reorder-features may change the order of features within a layer
    assertEquals(new HashSet<>(List.of(
      TestUtils.feature(newPoint(1, 2), "pois", Map.of("name", "a", "rank", 1L), 1),
      TestUtils.feature(newPoint(3, 4), "pois", Map.of("name", "b", "rank", 2L), 2),
      TestUtils.feature(newLineString(0, 0, 10, 10), "roads", Map.of("oneway", true), 3),
      TestUtils.feature(squareWithSquareHole(), "water", Map.of("area", 1.5), 4)
    )), new HashSet<>(decoded));
  }

  @Test
  void fastPforOptionReachesTheEncoder() {
    var points = IntStream.range(0, 300)
      .mapToObj(i -> feature("layer", i + 1, newPoint(i * 37 % 250, i * 91 % 250),
        Map.of("population", i * 7919L % 100_000)))
      .toList();
    var tile = new VectorTile().addLayerFeatures("layer", points);

    assertNotEquals(encode(tile).bytes().length, encode(tile, "--mlt-fastpfor").bytes().length);
  }

  @Test
  void fsstOptionReachesTheEncoder() {
    var points = IntStream.range(0, 300)
      .mapToObj(i -> feature("layer", i + 1, newPoint(i * 37 % 250, i * 91 % 250), Map.of("name", "Rue numero " + i)))
      .toList();
    var tile = new VectorTile().addLayerFeatures("layer", points);

    assertNotEquals(encode(tile).bytes().length, encode(tile, "--mlt-fsst").bytes().length);
  }

  @Test
  void tessellateOptionReachesTheEncoder() {
    var squares = IntStream.range(0, 300)
      .mapToObj(i -> feature("layer", i + 1, rectangle(i % 200, i % 200 + 3), Map.of()))
      .toList();
    var tile = new VectorTile().addLayerFeatures("layer", squares);

    assertNotEquals(encode(tile).bytes().length,
      encode(tile, "--mlt-tessellate-polygons", "--mlt-polygon-outline").bytes().length);
  }
}
