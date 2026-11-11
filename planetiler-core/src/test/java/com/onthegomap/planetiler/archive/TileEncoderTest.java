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
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments.ArgumentSet;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.util.AffineTransformation;
import org.maplibre.mlt.decoder.MltDecoder;

class TileEncoderTest {

  private static final boolean WITH_IDS = true;
  private static final boolean WITHOUT_IDS = false;
  private static final Long NO_ID = null;
  private static final Runnable MISSING_LIBRARY = () -> {
    throw new UnsatisfiedLinkError("no native library for this platform");
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
  void mltEncoderWritesTheTileAsMlt() {
    var tile = new VectorTile()
      .addLayerFeatures("poi", List.of(point("poi", 1, 10, 20, Map.of("name", "cafe"))))
      .addLayerFeatures("road", List.of(point("road", 2, 30, 40, Map.of("kind", "path"))));

    var encoded = encode(tile, WITH_IDS, "tile_format", "mlt");

    assertEquals(List.of(
      new DecodedFeature("poi", 1L, newPoint(10, 20), Map.of("name", "cafe")),
      new DecodedFeature("road", 2L, newPoint(30, 40), Map.of("kind", "path"))
    ), decodeMlt(encoded.bytes()));
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
  void mltEncoderLeavesOutIdsWhenAskedTo() {
    var tile = new VectorTile()
      .addLayerFeatures("poi", List.of(point("poi", 1, 10, 20, Map.of("name", "cafe"))));

    var encoded = encode(tile, WITHOUT_IDS, "tile_format", "mlt");

    assertEquals(List.of(
      new DecodedFeature("poi", NO_ID, newPoint(10, 20), Map.of("name", "cafe"))
    ), decodeMlt(encoded.bytes()));
  }

  @Test
  void mltEncoderWritesMixedValueTypesOfOneKeyAsText() {
    var tile = new VectorTile()
      .addLayerFeatures("building", List.of(
        point("building", 1, 10, 20, Map.of("height", 12)),
        point("building", 2, 30, 40, Map.of("height", "tall"))
      ));

    var encoded = encode(tile, WITH_IDS, "tile_format", "mlt");

    assertEquals(List.of(
      new DecodedFeature("building", 1L, newPoint(10, 20), Map.of("height", "12")),
      new DecodedFeature("building", 2L, newPoint(30, 40), Map.of("height", "tall"))
    ), decodeMlt(encoded.bytes()));
  }

  @Test
  void mltEncoderWithSharedDictionariesWritesMixedValueTypesOfOneKeyAsText() {
    // 100 repeated "cafe" strings are enough for shared dictionaries to kick in
    // layers come back in name order, so "building" is expected before "poi"
    List<VectorTile.Feature> manyCafes = new ArrayList<>();
    List<DecodedFeature> expected = new ArrayList<>(List.of(
      new DecodedFeature("building", 101L, newPoint(10, 20), Map.of("height", "12")),
      new DecodedFeature("building", 102L, newPoint(30, 40), Map.of("height", "tall"))
    ));
    for (int id = 1; id <= 100; id++) {
      manyCafes.add(point("poi", id, id, id, Map.of("name", "cafe")));
      expected.add(new DecodedFeature("poi", (long) id, newPoint(id, id), Map.of("name", "cafe")));
    }
    // "height" holds a number and a string, so it is left out of the shared dictionaries
    var tile = new VectorTile()
      .addLayerFeatures("poi", manyCafes)
      .addLayerFeatures("building", List.of(
        point("building", 101, 10, 20, Map.of("height", 12)),
        point("building", 102, 30, 40, Map.of("height", "tall"))
      ));

    var encoded = encode(tile, WITH_IDS, "tile_format", "mlt", "mlt_shared_dict", "true");

    assertEquals(expected, decodeMlt(encoded.bytes()));
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

    var encoded = encode(tile, WITH_IDS, "tile_format", tileFormat);

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

  @ParameterizedTest
  @ValueSource(strings = {"auto", "native"})
  void createsNativeMltEncoderWhereTheLibraryLoads(String mltEncoder) {
    assumeNativeMlt();

    try (var encoder = TileEncoder.factory(mltConfig(mltEncoder), Stats.inMemory()).get()) {
      assertInstanceOf(NativeMltTileEncoder.class, encoder);
    }
  }

  @Test
  void createsJavaMltEncoderWithoutLoadingTheNativeLibrary() {
    try (var encoder = create(mltConfig("java"), () -> fail("loaded the native library"))) {
      assertInstanceOf(JavaMltTileEncoder.class, encoder);
    }
  }

  private static Stream<ArgumentSet> libraryLoadFailures() {
    return Stream.of(
      argumentSet("missing library", MISSING_LIBRARY),
      argumentSet("native access denied", (Runnable) () -> {
        throw new IllegalCallerException("native access denied");
      }));
  }

  @ParameterizedTest
  @MethodSource("libraryLoadFailures")
  void autoFallsBackToJavaWhereTheLibraryDoesNotLoad(Runnable loadNativeLibrary) {
    try (var encoder = create(mltConfig("auto"), loadNativeLibrary)) {
      assertInstanceOf(JavaMltTileEncoder.class, encoder);
    }
  }

  @ParameterizedTest
  @MethodSource("libraryLoadFailures")
  void nativeFailsWhereTheLibraryDoesNotLoad(Runnable loadNativeLibrary) {
    var config = mltConfig("native");

    assertThrows(IllegalStateException.class, () -> create(config, loadNativeLibrary));
  }

  @Test
  void rejectsUnknownMltEncoder() {
    assertThrows(IllegalArgumentException.class, () -> mltConfig("rust"));
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
    return TileEncoder.factory(config, Stats.inMemory(), loadNativeLibrary).get();
  }

  /** Encodes MLT with the Java encoder, which {@link #decodeMlt} reads. */
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

  private static List<DecodedFeature> decodeMlt(byte[] bytes) {
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
