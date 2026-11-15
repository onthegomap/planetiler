package com.onthegomap.planetiler.archive;

import static com.onthegomap.planetiler.TestUtils.assumeNativeMlt;
import static com.onthegomap.planetiler.TestUtils.decodeMvt;
import static com.onthegomap.planetiler.TestUtils.feature;
import static com.onthegomap.planetiler.TestUtils.newPoint;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.params.provider.Arguments.argumentSet;

import com.onthegomap.planetiler.VectorTile;
import com.onthegomap.planetiler.config.Arguments;
import com.onthegomap.planetiler.config.PlanetilerConfig;
import com.onthegomap.planetiler.stats.Stats;
import com.onthegomap.planetiler.util.TileSizeStats;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments.ArgumentSet;
import org.junit.jupiter.params.provider.MethodSource;

/** Tests the MVT encoder and which encoder {@link TileEncoder#factory} picks, see {@link MltTileEncoderTest} for MLT. */
class TileEncoderTest {

  @Test
  void mvtEncoderWritesTheTileAsMvt() {
    var tile = new VectorTile()
      .addLayerFeatures("poi", List.of(point("poi", 1, 10, 20, Map.of("name", "cafe"))))
      .addLayerFeatures("road", List.of(point("road", 2, 30, 40, Map.of("kind", "path"))));

    var encoded = encodeMvt(tile, true);

    assertEquals(List.of(
      feature(newPoint(10, 20), "poi", Map.of("name", "cafe"), 1),
      feature(newPoint(30, 40), "road", Map.of("kind", "path"), 2)
    ), decodeMvt(encoded.bytes()));
  }

  @Test
  void mvtEncoderLeavesOutIdsWhenAskedTo() {
    var tile = new VectorTile()
      .addLayerFeatures("poi", List.of(point("poi", 1, 10, 20, Map.of("name", "cafe"))));

    var encoded = encodeMvt(tile, false);

    // MVT has no "missing id": a feature without one decodes with id 0
    assertEquals(List.of(feature(newPoint(10, 20), "poi", Map.of("name", "cafe"), 0)), decodeMvt(encoded.bytes()));
  }

  @Test
  void mvtEncoderReportsTheFeatureCountOfEveryLayer() {
    var tile = new VectorTile()
      .addLayerFeatures("poi", List.of(
        point("poi", 1, 10, 20, Map.of("name", "cafe")),
        point("poi", 2, 30, 40, Map.of("name", "bar"))
      ))
      .addLayerFeatures("road", List.of(point("road", 3, 50, 60, Map.of("kind", "path"))));

    var encoded = encodeMvt(tile, true);

    assertEquals(Map.of("poi", 2, "road", 1), encoded.layerStats().stream()
      .collect(Collectors.toMap(TileSizeStats.LayerStats::layer, TileSizeStats.LayerStats::layerFeatures)));
  }

  @Test
  void encodedComparesBytesByContent() {
    var stats = List.of(new TileSizeStats.LayerStats("poi", 10, 1, 1, 4, 1, 1));
    var encoded = new TileEncoder.Encoded(new byte[]{1, 2, 3}, stats);

    assertEquals(new TileEncoder.Encoded(new byte[]{1, 2, 3}, stats), encoded);
    assertEquals(new TileEncoder.Encoded(new byte[]{1, 2, 3}, stats).hashCode(), encoded.hashCode());
    assertNotEquals(new TileEncoder.Encoded(new byte[]{1, 2, 4}, stats), encoded);
    assertEquals("Encoded{bytes=byte[3], layerStats=" + stats + "}", encoded.toString());
  }

  @Test
  void createsNativeMltEncoderByDefault() {
    assumeNativeMlt();

    try (var encoder = create(config("tile_format", "mlt"))) {
      assertInstanceOf(NativeMltTileEncoder.class, encoder);
    }
  }

  @Test
  void createsJavaMltEncoderWhenAskedTo() {
    try (var encoder = create(config("tile_format", "mlt", "mlt_encoder", "java"))) {
      assertInstanceOf(JavaMltTileEncoder.class, encoder);
    }
  }

  private static Stream<ArgumentSet> nativeEncoderFailures() {
    return Stream.of(
      argumentSet("missing library", (Runnable) () -> {
        throw new UnsatisfiedLinkError("no native library for this platform");
      }),
      argumentSet("native access denied", (Runnable) () -> {
        throw new IllegalCallerException("native access denied");
      }));
  }

  @ParameterizedTest
  @MethodSource("nativeEncoderFailures")
  void nativeCheckSuggestsTheJavaEncoderWhereTheNativeOneDoesNotWork(Runnable encodeOneTile) {
    var error = assertThrows(IllegalStateException.class, () -> NativeMltTileEncoder.check(encodeOneTile));

    assertTrue(error.getMessage().contains("--mlt-encoder=java"), error.getMessage());
  }

  @Test
  void rejectsUnknownMltEncoder() {
    assertThrows(IllegalArgumentException.class, () -> config("tile_format", "mlt", "mlt_encoder", "auto"));
  }

  private static VectorTile.Feature point(String layer, long id, double x, double y, Map<String, Object> tags) {
    return new VectorTile.Feature(layer, id, VectorTile.encodeGeometry(newPoint(x, y)), tags);
  }

  private static PlanetilerConfig config(String... args) {
    return PlanetilerConfig.from(Arguments.of((Object[]) args));
  }

  private static TileEncoder create(PlanetilerConfig config) {
    return TileEncoder.factory(config, Stats.inMemory()).get();
  }

  private static TileEncoder.Encoded encodeMvt(VectorTile tile, boolean includeIds) {
    try (var encoder = create(config("tile_format", "mvt"))) {
      return encoder.encode(tile, includeIds);
    }
  }
}
