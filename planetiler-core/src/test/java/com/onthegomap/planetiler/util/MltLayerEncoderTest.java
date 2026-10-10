package com.onthegomap.planetiler.util;

import static com.onthegomap.planetiler.TestUtils.decodeSilently;
import static com.onthegomap.planetiler.TestUtils.newLineString;
import static com.onthegomap.planetiler.TestUtils.newMultiLineString;
import static com.onthegomap.planetiler.TestUtils.newMultiPoint;
import static com.onthegomap.planetiler.TestUtils.newMultiPolygon;
import static com.onthegomap.planetiler.TestUtils.newPoint;
import static com.onthegomap.planetiler.TestUtils.newPolygon;
import static com.onthegomap.planetiler.TestUtils.rectangle;
import static com.onthegomap.planetiler.TestUtils.rectangleCoordList;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.onthegomap.planetiler.TestUtils;
import com.onthegomap.planetiler.TestUtils.ComparableFeature;
import com.onthegomap.planetiler.VectorTile;
import com.onthegomap.planetiler.config.Arguments;
import com.onthegomap.planetiler.config.PlanetilerConfig;
import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Named;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.locationtech.jts.geom.Geometry;
import org.maplibre.mlt.ffi.MltConverter;

class MltLayerEncoderTest {

  private final MltLayerEncoder encoder = new MltLayerEncoder(PlanetilerConfig.defaults());

  @AfterEach
  void closeEncoder() {
    encoder.close();
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

  private static List<ComparableFeature> decode(byte[] mlt) {
    return VectorTile.decode(MltConverter.mltToMvt(mlt)).stream()
      .map(f -> TestUtils.feature(decodeSilently(f.geometry()), f.layer(), f.tags(), f.id()))
      .toList();
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

  @ParameterizedTest
  @MethodSource("geometries")
  void preservesGeometry(Geometry geometry) {
    var tile = tileWith(feature("layer", 1, geometry, Map.of()));

    var decoded = decode(encoder.encode(tile, true));

    assertEquals(List.of(TestUtils.feature(geometry, "layer", Map.of(), 1)), decoded);
  }

  @Test
  void preservesAttributesOfEveryType() {
    var tile = tileWith(feature("layer", 1, newPoint(1, 2),
      Map.of("string", "a", "integer", 1, "boolean", true, "float", 1.5f, "double", 2.25)));

    var decoded = decode(encoder.encode(tile, true));

    assertEquals(List.of(TestUtils.feature(newPoint(1, 2), "layer",
      Map.of("string", "a", "integer", 1L, "boolean", true, "float", 1.5f, "double", 2.25), 1)), decoded);
  }

  @Test
  void preservesLayersInNameOrderAndFeatureIds() {
    var tile = tileWith(
      feature("roads", 2, newLineString(0, 0, 10, 10), Map.of()),
      feature("pois", 1, newPoint(1, 2), Map.of()),
      feature("pois", 3, newPoint(3, 4), Map.of()));

    var decoded = decode(encoder.encode(tile, true));

    assertEquals(List.of(
      TestUtils.feature(newPoint(1, 2), "pois", Map.of(), 1),
      TestUtils.feature(newPoint(3, 4), "pois", Map.of(), 3),
      TestUtils.feature(newLineString(0, 0, 10, 10), "roads", Map.of(), 2)
    ), decoded);
  }

  @Test
  void leavesOutIdsWhenAskedTo() {
    var tile = tileWith(feature("layer", 7, newPoint(1, 2), Map.of()));

    var decoded = decode(encoder.encode(tile, false));

    assertEquals(List.of(TestUtils.feature(newPoint(1, 2), "layer", Map.of(), VectorTile.NO_FEATURE_ID)), decoded);
  }

  @Test
  void mixedTextAndIntegerValuesOfOneKeyBecomeText() {
    var tile = tileWith(
      feature("layer", 1, newPoint(1, 2), Map.of("ref", "A1")),
      feature("layer", 2, newPoint(3, 4), Map.of("ref", 12)));

    var decoded = decode(encoder.encode(tile, true));

    assertEquals(List.of(
      TestUtils.feature(newPoint(1, 2), "layer", Map.of("ref", "A1"), 1),
      TestUtils.feature(newPoint(3, 4), "layer", Map.of("ref", "12"), 2)
    ), decoded);
  }

  @Test
  void mixedTextIntegerAndFloatValuesOfOneKeyBecomeText() {
    var tile = tileWith(
      feature("layer", 1, newPoint(1, 2), Map.of("ref", "A1")),
      feature("layer", 2, newPoint(3, 4), Map.of("ref", 12)),
      feature("layer", 3, newPoint(5, 6), Map.of("ref", 1.5)));

    var decoded = decode(encoder.encode(tile, true));

    assertEquals(List.of(
      TestUtils.feature(newPoint(1, 2), "layer", Map.of("ref", "A1"), 1),
      TestUtils.feature(newPoint(3, 4), "layer", Map.of("ref", "12"), 2),
      TestUtils.feature(newPoint(5, 6), "layer", Map.of("ref", "1.5"), 3)
    ), decoded);
  }

  @Test
  void mixedIntegerAndFloatValuesOfOneKeyWidenToDouble() {
    var tile = tileWith(
      feature("layer", 1, newPoint(1, 2), Map.of("width", 3)),
      feature("layer", 2, newPoint(3, 4), Map.of("width", 1.5)));

    var decoded = decode(encoder.encode(tile, true));

    assertEquals(List.of(
      TestUtils.feature(newPoint(1, 2), "layer", Map.of("width", 3.0), 1),
      TestUtils.feature(newPoint(3, 4), "layer", Map.of("width", 1.5), 2)
    ), decoded);
  }

  @Test
  void reusedEncoderDoesNotCarryFeaturesIntoTheNextTile() {
    encoder.encode(tileWith(feature("first", 1, newPoint(1, 2), Map.of("name", "first"))), true);

    var second = decode(encoder.encode(tileWith(feature("second", 2, newPoint(3, 4), Map.of("ref", 2))), true));

    assertEquals(List.of(TestUtils.feature(newPoint(3, 4), "second", Map.of("ref", 2L), 2)), second);
  }

  @ParameterizedTest
  @ValueSource(strings = {
    "--mlt-advanced",
    "--mlt-fsst",
    "--mlt-fastpfor",
    "--mlt-reorder-features",
    "--mlt-shared-dict",
    "--mlt-tessellate-polygons --mlt-polygon-outline",
    "--mlt-fastpfor --mlt-fsst --mlt-reorder-features --mlt-shared-dict --mlt-tessellate-polygons --mlt-polygon-outline",
  })
  void mltOptionsDoNotChangeTheDecodedFeatures(String mltOptions) {
    var config = PlanetilerConfig.from(Arguments.fromArgs(("--tile-format=mlt " + mltOptions).split(" ")));
    var tile = tileWith(
      feature("pois", 1, newPoint(1, 2), Map.of("name", "a", "rank", 1)),
      feature("pois", 2, newPoint(3, 4), Map.of("name", "b", "rank", 2)),
      feature("roads", 3, newLineString(0, 0, 10, 10), Map.of("oneway", true)),
      feature("water", 4, squareWithSquareHole(),
        Map.of("area", 1.5)));

    List<ComparableFeature> decoded;
    try (var configuredEncoder = new MltLayerEncoder(config)) {
      decoded = decode(configuredEncoder.encode(tile, true));
    }

    // --mlt-reorder-features may change the order of features within a layer
    assertEquals(new HashSet<>(List.of(
      TestUtils.feature(newPoint(1, 2), "pois", Map.of("name", "a", "rank", 1L), 1),
      TestUtils.feature(newPoint(3, 4), "pois", Map.of("name", "b", "rank", 2L), 2),
      TestUtils.feature(newLineString(0, 0, 10, 10), "roads", Map.of("oneway", true), 3),
      TestUtils.feature(squareWithSquareHole(), "water",
        Map.of("area", 1.5), 4)
    )), new HashSet<>(decoded));
  }

  @Test
  void encodersOnSeparateThreadsDoNotInterfere() throws Exception {
    int threads = 8;
    try (var pool = Executors.newFixedThreadPool(threads)) {
      List<Callable<List<ComparableFeature>>> tasks = new ArrayList<>();
      for (int thread = 0; thread < threads; thread++) {
        long id = thread + 1;
        tasks.add(() -> {
          var tile = tileWith(feature("layer", id, newPoint(id, id), Map.of("thread", id)));
          List<ComparableFeature> decoded = List.of();
          try (var threadEncoder = new MltLayerEncoder(PlanetilerConfig.defaults())) {
            for (int i = 0; i < 200; i++) {
              decoded = decode(threadEncoder.encode(tile, true));
            }
          }
          return decoded;
        });
      }

      var results = pool.invokeAll(tasks);

      for (int thread = 0; thread < threads; thread++) {
        long id = thread + 1;
        assertEquals(List.of(TestUtils.feature(newPoint(id, id), "layer", Map.of("thread", id), id)),
          results.get(thread).get());
      }
    }
  }

  /** Opt-in check against real tiles: {@code -Dmlt.test.mbtiles=path/to/file.mbtiles}. */
  @Test
  void everyTileOfAnMbtilesFileDecodesToTheSameFeaturesAsMvt() throws Exception {
    String mbtiles = System.getProperty("mlt.test.mbtiles");
    Assumptions.assumeTrue(mbtiles != null, "set -Dmlt.test.mbtiles to run");
    try (var connection = DriverManager.getConnection("jdbc:sqlite:" + mbtiles);
      var rs = connection.createStatement().executeQuery("select tile_data from tiles")) {
      while (rs.next()) {
        var tile = new VectorTile();
        VectorTile.decode(Gzip.gunzip(rs.getBytes(1))).stream()
          .collect(Collectors.groupingBy(VectorTile.Feature::layer))
          .forEach(tile::addLayerFeatures);
        var decodedFromMvt = VectorTile.decode(tile.encode()).stream()
          .map(f -> TestUtils.feature(decodeSilently(f.geometry()), f.layer(), f.tags(), f.id()))
          .toList();

        var decodedFromMlt = decode(encoder.encode(tile, true));

        assertEquals(decodedFromMvt, decodedFromMlt);
      }
    }
  }
}
