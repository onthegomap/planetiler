package com.onthegomap.planetiler.config;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

class PlanetilerConfigTest {

  @ParameterizedTest
  @ValueSource(strings = {
    "--tile-format=mvt",
    "--tile-format=mvt --mlt-fsst=false",
    "--tile-format=mlt",
    "--tile-format=mlt --mlt-advanced",
    "--tile-format=mlt --mlt-tessellate-polygons",
    "--tile-format=mlt --mlt-fastpfor --mlt-fsst --mlt-reorder-features --mlt-shared-dict --exclude-ids --mlt-tessellate-polygons --mlt-polygon-outline",
  })
  void acceptsOptions(String args) {
    assertDoesNotThrow(() -> PlanetilerConfig.from(
      Arguments.fromArgs(args.split("\\s"))
    ));
  }

  @ParameterizedTest
  @ValueSource(strings = {
    "--mlt-fsst",
    "--mlt-fastpfor",
    "--mlt-tessellate-polygons",
    "--mlt-polygon-outline",
    "--mlt-reorder-features",
    "--mlt-shared-dict",
    "--mlt-advanced",
  })
  void rejectsMltOptionWithMvt(String mltOption) {
    var arguments = Arguments.fromArgs("--tile-format=mvt", mltOption);

    var error = assertThrows(IllegalArgumentException.class, () -> PlanetilerConfig.from(arguments));

    assertEquals("mlt_* options require tile_format=mlt, was mvt", error.getMessage());
  }

  @ParameterizedTest
  @CsvSource({
    "--tile-format=mlt --mlt-fsst, true, false",
    "--tile-format=mlt --mlt-fastpfor, false, true",
    "--tile-format=mlt --mlt-advanced, true, true",
  })
  void mltAdvancedEncodings(String args, boolean fsst, boolean fastPfor) {
    var config = PlanetilerConfig.from(
      Arguments.fromArgs(args.split("\\s"))
    );

    assertEquals(fsst, config.mltFsst());
    assertEquals(fastPfor, config.mltFastPfor());
  }
}
