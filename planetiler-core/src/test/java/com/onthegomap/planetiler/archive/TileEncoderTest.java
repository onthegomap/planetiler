package com.onthegomap.planetiler.archive;

import static com.onthegomap.planetiler.TestUtils.assumeNativeMlt;
import static com.onthegomap.planetiler.TestUtils.decodeSilently;
import static com.onthegomap.planetiler.TestUtils.newPoint;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;
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
import org.locationtech.jts.geom.Geometry;

/** Tests the MVT encoder and which encoder {@link TileEncoder#factory} picks, see {@link MltTileEncoderTest} for MLT. */
class TileEncoderTest {

  private static final boolean WITH_IDS = true;
  private static final boolean WITHOUT_IDS = false;

  @Test
  void mvtEncoderWritesTheTileAsMvt() {
    var tile = new VectorTile()
      .addLayerFeatures("poi", List.of(point("poi", 1, 10, 20, Map.of("name", "cafe"))))
      .addLayerFeatures("road", List.of(point("road", 2, 30, 40, Map.of("kind", "path"))));

    var encoded = encodeMvt(tile, WITH_IDS);

    assertEquals(List.of(
      new DecodedFeature("poi", 1L, newPoint(10, 20), Map.of("name", "cafe")),
      new DecodedFeature("road", 2L, newPoint(30, 40), Map.of("kind", "path"))
    ), decodeMvt(encoded.bytes()));
  }

  @Test
  void mvtEncoderLeavesOutIdsWhenAskedTo() {
    var tile = new VectorTile()
      .addLayerFeatures("poi", List.of(point("poi", 1, 10, 20, Map.of("name", "cafe"))));

    var encoded = encodeMvt(tile, WITHOUT_IDS);

    // MVT has no "missing id": a feature without one decodes with id 0
    assertEquals(List.of(
      new DecodedFeature("poi", 0L, newPoint(10, 20), Map.of("name", "cafe"))
    ), decodeMvt(encoded.bytes()));
  }

  @Test
  void mvtEncoderReportsTheFeatureCountOfEveryLayer() {
    var tile = new VectorTile()
      .addLayerFeatures("poi", List.of(
        point("poi", 1, 10, 20, Map.of("name", "cafe")),
        point("poi", 2, 30, 40, Map.of("name", "bar"))
      ))
      .addLayerFeatures("road", List.of(point("road", 3, 50, 60, Map.of("kind", "path"))));

    var encoded = encodeMvt(tile, WITH_IDS);

    assertEquals(
      Map.of("poi", 2, "road", 1),
      featureCountByLayer(encoded.layerStats())
    );
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

    try (var encoder = TileEncoder.factory(config("tile_format", "mlt"), Stats.inMemory()).get()) {
      assertInstanceOf(NativeMltTileEncoder.class, encoder);
    }
  }

  @Test
  void createsJavaMltEncoderWithoutCheckingTheNativeEncoder() {
    try (var encoder = create(mltConfig("java"), () -> fail("checked the native encoder"))) {
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
  void nativeFailsWhereTheNativeEncoderDoesNotWork(Runnable checkNativeEncoder) {
    var config = mltConfig("native");

    assertThrows(IllegalStateException.class, () -> create(config, checkNativeEncoder));
  }

  @Test
  void rejectsUnknownMltEncoder() {
    assertThrows(IllegalArgumentException.class, () -> mltConfig("auto"));
  }

  /** A feature as read back from encoded tile bytes, with geometry in tile coordinates (0-256). */
  private record DecodedFeature(String layer, Long id, Geometry geometry, Map<String, Object> tags) {}

  private static VectorTile.Feature point(String layer, long id, double x, double y, Map<String, Object> tags) {
    return new VectorTile.Feature(layer, id, VectorTile.encodeGeometry(newPoint(x, y)), tags);
  }

  private static PlanetilerConfig config(String... args) {
    return PlanetilerConfig.from(Arguments.of((Object[]) args));
  }

  private static PlanetilerConfig mltConfig(String mltEncoder) {
    return config("tile_format", "mlt", "mlt_encoder", mltEncoder);
  }

  private static TileEncoder create(PlanetilerConfig config, Runnable checkNativeEncoder) {
    return TileEncoder.factory(config, Stats.inMemory(), checkNativeEncoder).get();
  }

  private static TileEncoder.Encoded encodeMvt(VectorTile tile, boolean includeIds) {
    try (var encoder = create(config("tile_format", "mvt"), () -> fail("checked the native encoder"))) {
      return encoder.encode(tile, includeIds);
    }
  }

  private static List<DecodedFeature> decodeMvt(byte[] bytes) {
    return VectorTile.decode(bytes).stream()
      .map(f -> new DecodedFeature(f.layer(), f.id(), decodeSilently(f.geometry()), f.tags()))
      .toList();
  }

  private static Map<String, Integer> featureCountByLayer(List<TileSizeStats.LayerStats> layerStats) {
    return layerStats.stream()
      .collect(Collectors.toMap(TileSizeStats.LayerStats::layer,
        TileSizeStats.LayerStats::layerFeatures));
  }
}
