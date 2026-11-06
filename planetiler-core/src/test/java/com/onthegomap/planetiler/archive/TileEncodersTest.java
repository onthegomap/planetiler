package com.onthegomap.planetiler.archive;

import static com.onthegomap.planetiler.TestUtils.assumeNativeMlt;
import static com.onthegomap.planetiler.TestUtils.decodeSilently;
import static com.onthegomap.planetiler.TestUtils.newPoint;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.onthegomap.planetiler.VectorTile;
import com.onthegomap.planetiler.config.Arguments;
import com.onthegomap.planetiler.config.PlanetilerConfig;
import com.onthegomap.planetiler.stats.Stats;
import com.onthegomap.planetiler.util.TileSizeStats;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.util.AffineTransformation;
import org.maplibre.mlt.decoder.MltDecoder;

class TileEncodersTest {

  private static final boolean WITH_IDS = true;
  private static final boolean WITHOUT_IDS = false;
  private static final Long NO_ID = null;
  private static final Runnable MISSING_LIBRARY = () -> {
    throw new UnsatisfiedLinkError("no native library for this platform");
  };
  private static final Runnable NATIVE_ACCESS_DENIED = () -> {
    throw new IllegalCallerException("native access denied");
  };

  @Test
  void mvtEncoderWritesTheTileAsMvt() {
    var tile = new VectorTile()
      .addLayerFeatures("poi", List.of(point("poi", 1, 10, 20, Map.of("name", "cafe"))))
      .addLayerFeatures("road", List.of(point("road", 2, 30, 40, Map.of("kind", "path"))));

    var encoded = encode(tile, WITH_IDS, "tile_format", "mvt");

    assertEquals(List.of(
      new DecodedFeature("poi", 1L, newPoint(10, 20), Map.of("name", "cafe")),
      new DecodedFeature("road", 2L, newPoint(30, 40), Map.of("kind", "path"))
    ), decodeMvt(encoded.bytes()));
  }

  @Test
  void javaMltEncoderWritesTheTileAsMlt() {
    var tile = new VectorTile()
      .addLayerFeatures("poi", List.of(point("poi", 1, 10, 20, Map.of("name", "cafe"))))
      .addLayerFeatures("road", List.of(point("road", 2, 30, 40, Map.of("kind", "path"))));

    var encoded = encode(tile, WITH_IDS, "tile_format", "mlt", "mlt_encoder", "java");

    assertEquals(List.of(
      new DecodedFeature("poi", 1L, newPoint(10, 20), Map.of("name", "cafe")),
      new DecodedFeature("road", 2L, newPoint(30, 40), Map.of("kind", "path"))
    ), decodeJavaMlt(encoded.bytes()));
  }

  @Test
  void mvtEncoderLeavesOutIdsWhenAskedTo() {
    var tile = new VectorTile()
      .addLayerFeatures("poi", List.of(point("poi", 1, 10, 20, Map.of("name", "cafe"))));

    var encoded = encode(tile, WITHOUT_IDS, "tile_format", "mvt");

    // MVT has no "missing id": a feature without one decodes with id 0
    assertEquals(List.of(
      new DecodedFeature("poi", 0L, newPoint(10, 20), Map.of("name", "cafe"))
    ), decodeMvt(encoded.bytes()));
  }

  @Test
  void javaMltEncoderLeavesOutIdsWhenAskedTo() {
    var tile = new VectorTile()
      .addLayerFeatures("poi", List.of(point("poi", 1, 10, 20, Map.of("name", "cafe"))));

    var encoded = encode(tile, WITHOUT_IDS, "tile_format", "mlt", "mlt_encoder", "java");

    assertEquals(List.of(
      new DecodedFeature("poi", NO_ID, newPoint(10, 20), Map.of("name", "cafe"))
    ), decodeJavaMlt(encoded.bytes()));
  }

  @Test
  void javaMltEncoderWritesMixedValueTypesOfOneKeyAsText() {
    var tile = new VectorTile()
      .addLayerFeatures("building", List.of(
        point("building", 1, 10, 20, Map.of("height", 12)),
        point("building", 2, 30, 40, Map.of("height", "tall"))
      ));

    var encoded = encode(tile, WITH_IDS, "tile_format", "mlt", "mlt_encoder", "java");

    assertEquals(List.of(
      new DecodedFeature("building", 1L, newPoint(10, 20), Map.of("height", "12")),
      new DecodedFeature("building", 2L, newPoint(30, 40), Map.of("height", "tall"))
    ), decodeJavaMlt(encoded.bytes()));
  }

  @Test
  void javaMltEncoderWithSharedDictionariesWritesTheSameFeatures() {
    // shared dictionaries only kick in once a tile repeats enough strings
    List<VectorTile.Feature> manyCafes = new ArrayList<>();
    List<DecodedFeature> expected = new ArrayList<>();
    for (int id = 1; id <= 100; id++) {
      manyCafes.add(point("poi", id, id, id, Map.of("name", "cafe", "kind", "food")));
      expected.add(new DecodedFeature("poi", (long) id, newPoint(id, id), Map.of("name", "cafe", "kind", "food")));
    }
    var tile = new VectorTile().addLayerFeatures("poi", manyCafes);

    var encoded =
      encode(tile, WITH_IDS, "tile_format", "mlt", "mlt_encoder", "java", "mlt_shared_dict", "true");

    assertEquals(expected, decodeJavaMlt(encoded.bytes()));
  }

  @ParameterizedTest
  @ValueSource(strings = {"mvt", "mlt"})
  void reportsTheFeatureCountOfEveryLayer(String tileFormat) {
    var tile = new VectorTile()
      .addLayerFeatures("poi", List.of(
        point("poi", 1, 10, 20, Map.of("name", "cafe")),
        point("poi", 2, 30, 40, Map.of("name", "bar"))
      ))
      .addLayerFeatures("road", List.of(point("road", 3, 50, 60, Map.of("kind", "path"))));

    var encoded = encode(tile, WITH_IDS, "tile_format", tileFormat, "mlt_encoder", "java");

    assertEquals(
      Map.of("poi", 2, "road", 1),
      featureCountByLayer(encoded.layerStats())
    );
  }

  @ParameterizedTest
  @ValueSource(strings = {"mvt", "mlt"})
  void factoryGivesEveryThreadItsOwnEncoder(String tileFormat) {
    var config = config("tile_format", tileFormat, "mlt_encoder", "java");
    var factory = TileEncoders.factory(config, Stats.inMemory(), MISSING_LIBRARY);

    try (var encoderForThread1 = factory.get(); var encoderForThread2 = factory.get()) {
      assertNotSame(encoderForThread1, encoderForThread2);
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"auto", "native"})
  void createsNativeMltEncoderWhereTheLibraryLoads(String mltEncoder) {
    assumeNativeMlt();

    try (var encoder = TileEncoders.factory(mltConfig(mltEncoder), Stats.inMemory()).get()) {
      assertInstanceOf(NativeMltTileEncoder.class, encoder);
    }
  }

  @Test
  void createsJavaMltEncoderWithoutLoadingTheNativeLibrary() {
    var loaded = new AtomicBoolean(false);

    try (var encoder = create(mltConfig("java"), () -> loaded.set(true))) {
      assertInstanceOf(JavaMltTileEncoder.class, encoder);
    }
    assertFalse(loaded.get());
  }

  @Test
  void autoFallsBackToJavaOnPlatformsWithoutNativeLibrary() {
    try (var encoder = create(mltConfig("auto"), MISSING_LIBRARY)) {
      assertInstanceOf(JavaMltTileEncoder.class, encoder);
    }
  }

  @Test
  void autoFallsBackToJavaWhenNativeAccessIsDenied() {
    try (var encoder = create(mltConfig("auto"), NATIVE_ACCESS_DENIED)) {
      assertInstanceOf(JavaMltTileEncoder.class, encoder);
    }
  }

  @Test
  void autoDoesNotHideOtherNativeErrors() {
    var bug = new IllegalStateException("bug in the native library");
    Runnable buggyLibrary = () -> {
      throw bug;
    };
    var config = mltConfig("auto");

    var error = assertThrows(IllegalStateException.class, () -> create(config, buggyLibrary));

    assertSame(bug, error);
  }

  @Test
  void nativeFailsOnPlatformsWithoutNativeLibrary() {
    var config = mltConfig("native");

    var error = assertThrows(IllegalStateException.class, () -> create(config, MISSING_LIBRARY));

    assertInstanceOf(UnsatisfiedLinkError.class, error.getCause());
  }

  @Test
  void nativeFailureExplainsHowToAllowNativeAccess() {
    var config = mltConfig("native");

    var error = assertThrows(IllegalStateException.class, () -> create(config, NATIVE_ACCESS_DENIED));

    assertTrue(error.getMessage().contains("--enable-native-access=ALL-UNNAMED"), error.getMessage());
  }

  @Test
  void rejectsUnknownMltEncoder() {
    var error = assertThrows(IllegalArgumentException.class, () -> mltConfig("rust"));

    assertEquals("invalid MLT encoder ID rust; expected one of [auto, native, java]", error.getMessage());
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

  private static TileEncoder create(PlanetilerConfig config, Runnable loadNativeLibrary) {
    return TileEncoders.factory(config, Stats.inMemory(), loadNativeLibrary).get();
  }

  private static TileEncoder.Encoded encode(VectorTile tile, boolean includeIds, String... configArgs) {
    try (var encoder = create(config(configArgs), MISSING_LIBRARY)) {
      return encoder.encode(tile, includeIds);
    }
  }

  private static List<DecodedFeature> decodeMvt(byte[] bytes) {
    return VectorTile.decode(bytes).stream()
      .map(f -> new DecodedFeature(f.layer(), f.id(), decodeSilently(f.geometry()), f.tags()))
      .toList();
  }

  /** Decodes with the Java MLT decoder, which only reads tiles of the Java encoder. */
  private static List<DecodedFeature> decodeJavaMlt(byte[] bytes) {
    try {
      return MltDecoder.decodeMlTile(bytes).layers().stream()
        .flatMap(layer -> layer.features().stream().map(f -> new DecodedFeature(
          layer.name(),
          f.idOrNull(),
          toTileCoordinates(f.geometry(), layer.tileExtent()),
          f.properties()
        )))
        .toList();
    } catch (IOException e) {
      throw new AssertionError("encoded bytes are not a valid MLT tile", e);
    }
  }

  private static Geometry toTileCoordinates(Geometry geometry, int extent) {
    double scale = 256d / extent;
    return AffineTransformation.scaleInstance(scale, scale).transform(geometry);
  }

  private static Map<String, Integer> featureCountByLayer(List<TileSizeStats.LayerStats> layerStats) {
    return layerStats.stream()
      .collect(Collectors.toMap(TileSizeStats.LayerStats::layer,
        TileSizeStats.LayerStats::layerFeatures));
  }
}
