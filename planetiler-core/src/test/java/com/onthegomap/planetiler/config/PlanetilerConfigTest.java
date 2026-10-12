package com.onthegomap.planetiler.config;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class PlanetilerConfigTest {

  @Test
  void acceptsDefaultsWithoutAnyMltOptions() {
    assertDoesNotThrow(() -> PlanetilerConfig.from(
      Arguments.of()
    ));
  }

  @Test
  void acceptsMvtWithoutMltOptions() {
    assertDoesNotThrow(() -> PlanetilerConfig.from(
      Arguments.fromArgs("--tile-format=mvt")
    ));
  }

  @Test
  void acceptsMvtWithMltFsstExplicitlyDisabled() {
    assertDoesNotThrow(() -> PlanetilerConfig.from(
      Arguments.fromArgs("--tile-format=mvt", "--mlt-fsst=false")
    ));
  }

  @Test
  void acceptsMltWithoutMltOptions() {
    assertDoesNotThrow(() -> PlanetilerConfig.from(
      Arguments.fromArgs("--tile-format=mlt")
    ));
  }

  @Test
  void acceptsMltAdvanced() {
    assertDoesNotThrow(() -> PlanetilerConfig.from(
      Arguments.fromArgs("--tile-format=mlt", "--mlt-advanced")
    ));
  }

  @Test
  void acceptsMltTessellatePolygons() {
    assertDoesNotThrow(() -> PlanetilerConfig.from(
      Arguments.fromArgs("--tile-format=mlt", "--mlt-tessellate-polygons")
    ));
  }

  @Test
  void acceptsAllMltOptionsTogether() {
    assertDoesNotThrow(() -> PlanetilerConfig.from(
      Arguments.fromArgs(
        "--tile-format=mlt", "--mlt-fastpfor", "--mlt-fsst", "--mlt-reorder-features", "--mlt-shared-dict",
        "--exclude-ids", "--mlt-tessellate-polygons", "--mlt-polygon-outline")
    ));
  }

  @Test
  void rejectsMltAdvancedWithDefaultTileFormat() {
    var arguments = Arguments.fromArgs("--mlt-advanced");

    var error = assertThrows(IllegalArgumentException.class, () -> PlanetilerConfig.from(arguments));

    assertEquals("mlt_* options require tile_format=mlt, was mvt", error.getMessage());
  }

  @ParameterizedTest
  @ValueSource(strings = {
    "--mlt-fsst",
    "--mlt-fastpfor",
    "--mlt-tessellate-polygons",
    "--mlt-polygon-outline",
    "--mlt-reorder-features",
    "--mlt-shared-dict",
  })
  void rejectsMltOptionWithMvt(String mltOption) {
    var arguments = Arguments.fromArgs("--tile-format=mvt", mltOption);

    var error = assertThrows(IllegalArgumentException.class, () -> PlanetilerConfig.from(arguments));

    assertEquals("mlt_* options require tile_format=mlt, was mvt", error.getMessage());
  }

  @Test
  void mltFsstEnablesOnlyFsst() {
    var config = PlanetilerConfig.from(
      Arguments.fromArgs("--tile-format=mlt", "--mlt-fsst")
    );

    assertTrue(config.mltFsst());
    assertFalse(config.mltFastPfor());
  }

  @Test
  void mltFastPforEnablesOnlyFastPfor() {
    var config = PlanetilerConfig.from(
      Arguments.fromArgs("--tile-format=mlt", "--mlt-fastpfor")
    );

    assertFalse(config.mltFsst());
    assertTrue(config.mltFastPfor());
  }

  @Test
  void mltAdvancedEnablesFsstAndFastPfor() {
    var config = PlanetilerConfig.from(
      Arguments.fromArgs("--tile-format=mlt", "--mlt-advanced")
    );

    assertTrue(config.mltFsst());
    assertTrue(config.mltFastPfor());
  }
}
