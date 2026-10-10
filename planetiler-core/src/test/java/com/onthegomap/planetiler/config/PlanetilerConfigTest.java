package com.onthegomap.planetiler.config;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class PlanetilerConfigTest {

  private static PlanetilerConfig parse(String args) {
    return PlanetilerConfig.from(Arguments.fromArgs(args.isBlank() ? new String[0] : args.split(" ")));
  }

  /** Returns the option each warning is about, which is the first word of the warning. */
  private static List<String> optionsWarnedAbout(PlanetilerConfig config) {
    return config.mltWarnings().stream().map(warning -> warning.split(" ", 2)[0]).toList();
  }

  @ParameterizedTest
  @ValueSource(strings = {
    "",
    "--tile-format=mvt",
    "--tile-format=mvt --mlt-fsst=false",
    "--tile-format=mlt",
    "--tile-format=mlt --mlt-advanced",
    "--tile-format=mlt --mlt-tessellate-polygons --mlt-polygon-outline",
    "--tile-format=mlt --mlt-fastpfor --mlt-fsst --mlt-reorder-features --mlt-shared-dict --exclude-ids --mlt-tessellate-polygons --mlt-polygon-outline",
  })
  void acceptsValidMltOptions(String args) {
    assertDoesNotThrow(() -> parse(args));
  }

  @ParameterizedTest
  @ValueSource(strings = {
    "--mlt-advanced",
    "--tile-format=mvt --mlt-fsst",
    "--tile-format=mvt --mlt-fastpfor",
    "--tile-format=mvt --mlt-tessellate-polygons --mlt-polygon-outline",
    "--tile-format=mvt --mlt-reorder-features",
    "--tile-format=mvt --mlt-shared-dict",
  })
  void rejectsMltOptionsWithoutMltTileFormat(String args) {
    var error = assertThrows(IllegalArgumentException.class, () -> parse(args));

    assertEquals("mlt_* options require tile_format=mlt, was mvt", error.getMessage());
  }

  @Test
  void rejectsTessellationWithoutOutlinesSinceMltV1CannotStoreThem() {
    var error = assertThrows(IllegalArgumentException.class,
      () -> parse("--tile-format=mlt --mlt-tessellate-polygons"));

    assertEquals(
      "mlt_tessellate_polygons requires mlt_polygon_outline, MLT v1 cannot store polygon triangles without outlines",
      error.getMessage());
  }

  @Test
  void rejectsOutlinesWithoutTessellation() {
    var error = assertThrows(IllegalArgumentException.class, () -> parse("--tile-format=mlt --mlt-polygon-outline"));

    assertEquals("mlt_polygon_outline requires mlt_tessellate_polygons", error.getMessage());
  }

  @Test
  void mltFsstEnablesOnlyFsst() {
    var config = parse("--tile-format=mlt --mlt-fsst");

    assertTrue(config.mltFsst());
    assertFalse(config.mltFastPfor());
  }

  @Test
  void mltFastPforEnablesOnlyFastPfor() {
    var config = parse("--tile-format=mlt --mlt-fastpfor");

    assertFalse(config.mltFsst());
    assertTrue(config.mltFastPfor());
  }

  @Test
  void mltAdvancedEnablesFsstAndFastPfor() {
    var config = parse("--tile-format=mlt --mlt-advanced");

    assertTrue(config.mltFsst());
    assertTrue(config.mltFastPfor());
  }

  @Test
  void warnsThatFsstMakesGzippedTilesLarger() {
    var config = parse("--tile-format=mlt --mlt-fsst");

    assertEquals(List.of("mlt_fsst"), optionsWarnedAbout(config));
  }

  @Test
  void warnsThatFastPforMakesGzippedTilesLarger() {
    var config = parse("--tile-format=mlt --mlt-fastpfor");

    assertEquals(List.of("mlt_fastpfor"), optionsWarnedAbout(config));
  }

  @ParameterizedTest
  @ValueSource(strings = {
    "--tile-format=mlt --mlt-advanced",
    "--tile-format=mlt --mlt-advanced --tile-compression=gzip",
  })
  void warnsAboutBothFsstAndFastPforWithGzip(String args) {
    assertEquals(List.of("mlt_fsst", "mlt_fastpfor"), optionsWarnedAbout(parse(args)));
  }

  @Test
  void doesNotWarnAboutFsstOrFastPforWithoutCompression() {
    var config = parse("--tile-format=mlt --mlt-advanced --tile-compression=none");

    assertEquals(List.of(), config.mltWarnings());
  }

  @ParameterizedTest
  @ValueSource(strings = {
    "",
    "--tile-format=mlt",
    "--tile-format=mlt --mlt-reorder-features --mlt-shared-dict --mlt-tessellate-polygons --mlt-polygon-outline",
  })
  void doesNotWarnWhenFsstAndFastPforAreOff(String args) {
    assertEquals(List.of(), parse(args).mltWarnings());
  }
}
