package com.onthegomap.planetiler.config;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Property;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class PlanetilerConfigTest {

  private final List<String> warnings = new CopyOnWriteArrayList<>();
  private AbstractAppender appender;

  @BeforeEach
  void captureWarnings() {
    appender = new AbstractAppender("warnings", null, null, true, Property.EMPTY_ARRAY) {
      @Override
      public void append(LogEvent event) {
        if (event.getLevel() == Level.WARN) {
          warnings.add(event.getMessage().getFormattedMessage());
        }
      }
    };
    appender.start();
    var context = (LoggerContext) LogManager.getContext(false);
    context.getConfiguration().getLoggerConfig(PlanetilerConfig.class.getName()).addAppender(appender, null, null);
    context.updateLoggers();
  }

  @AfterEach
  void stopCapturingWarnings() {
    var context = (LoggerContext) LogManager.getContext(false);
    context.getConfiguration().getLoggerConfig(PlanetilerConfig.class.getName()).removeAppender("warnings");
    context.updateLoggers();
    appender.stop();
  }

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
    var error = assertThrows(IllegalArgumentException.class, () -> PlanetilerConfig.from(
      Arguments.fromArgs("--mlt-advanced")
    ));

    assertEquals("mlt_* options require tile_format=mlt, was mvt", error.getMessage());
  }

  @Test
  void rejectsMltFsstWithMvt() {
    var error = assertThrows(IllegalArgumentException.class, () -> PlanetilerConfig.from(
      Arguments.fromArgs("--tile-format=mvt", "--mlt-fsst")
    ));

    assertEquals("mlt_* options require tile_format=mlt, was mvt", error.getMessage());
  }

  @Test
  void rejectsMltFastPforWithMvt() {
    var error = assertThrows(IllegalArgumentException.class, () -> PlanetilerConfig.from(
      Arguments.fromArgs("--tile-format=mvt", "--mlt-fastpfor")
    ));

    assertEquals("mlt_* options require tile_format=mlt, was mvt", error.getMessage());
  }

  @Test
  void rejectsMltTessellatePolygonsWithMvt() {
    var error = assertThrows(IllegalArgumentException.class, () -> PlanetilerConfig.from(
      Arguments.fromArgs("--tile-format=mvt", "--mlt-tessellate-polygons", "--mlt-polygon-outline")
    ));

    assertEquals("mlt_* options require tile_format=mlt, was mvt", error.getMessage());
  }

  @Test
  void rejectsMltPolygonOutlineWithMvt() {
    var error = assertThrows(IllegalArgumentException.class, () -> PlanetilerConfig.from(
      Arguments.fromArgs("--tile-format=mvt", "--mlt-polygon-outline")
    ));

    assertEquals("mlt_* options require tile_format=mlt, was mvt", error.getMessage());
  }

  @Test
  void rejectsMltReorderFeaturesWithMvt() {
    var error = assertThrows(IllegalArgumentException.class, () -> PlanetilerConfig.from(
      Arguments.fromArgs("--tile-format=mvt", "--mlt-reorder-features")
    ));

    assertEquals("mlt_* options require tile_format=mlt, was mvt", error.getMessage());
  }

  @Test
  void rejectsMltSharedDictWithMvt() {
    var error = assertThrows(IllegalArgumentException.class, () -> PlanetilerConfig.from(
      Arguments.fromArgs("--tile-format=mvt", "--mlt-shared-dict")
    ));

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

  @Test
  void warnsThatFsstMakesGzippedTilesLarger() {
    PlanetilerConfig.from(
      Arguments.fromArgs("--tile-format=mlt", "--mlt-fsst")
    );

    assertEquals(List.of(
      "mlt_fsst usually makes gzip-compressed tiles larger, use tile_compression=none or disable it"
    ), warnings);
  }

  @Test
  void warnsThatFastPforMakesGzippedTilesLarger() {
    PlanetilerConfig.from(
      Arguments.fromArgs("--tile-format=mlt", "--mlt-fastpfor")
    );

    assertEquals(List.of(
      "mlt_fastpfor makes gzip-compressed tiles larger, use tile_compression=none or disable it"
    ), warnings);
  }

  @Test
  void warnsAboutFsstAndFastPforForMltAdvanced() {
    PlanetilerConfig.from(
      Arguments.fromArgs("--tile-format=mlt", "--mlt-advanced")
    );

    assertEquals(List.of(
      "mlt_fsst usually makes gzip-compressed tiles larger, use tile_compression=none or disable it",
      "mlt_fastpfor makes gzip-compressed tiles larger, use tile_compression=none or disable it"
    ), warnings);
  }

  @Test
  void warnsAboutFsstAndFastPforForExplicitGzip() {
    PlanetilerConfig.from(
      Arguments.fromArgs("--tile-format=mlt", "--mlt-advanced", "--tile-compression=gzip")
    );

    assertEquals(List.of(
      "mlt_fsst usually makes gzip-compressed tiles larger, use tile_compression=none or disable it",
      "mlt_fastpfor makes gzip-compressed tiles larger, use tile_compression=none or disable it"
    ), warnings);
  }

  @Test
  void doesNotWarnAboutFsstOrFastPforWithoutCompression() {
    PlanetilerConfig.from(
      Arguments.fromArgs("--tile-format=mlt", "--mlt-advanced", "--tile-compression=none")
    );

    assertEquals(List.of(), warnings);
  }

  @Test
  void doesNotWarnWithDefaults() {
    PlanetilerConfig.from(
      Arguments.of()
    );

    assertEquals(List.of(), warnings);
  }

  @Test
  void doesNotWarnForMltWithoutFsstOrFastPfor() {
    PlanetilerConfig.from(
      Arguments.fromArgs("--tile-format=mlt")
    );

    assertEquals(List.of(), warnings);
  }

  @Test
  void doesNotWarnForOtherMltOptions() {
    PlanetilerConfig.from(
      Arguments.fromArgs(
        "--tile-format=mlt", "--mlt-reorder-features", "--mlt-shared-dict", "--mlt-tessellate-polygons",
        "--mlt-polygon-outline")
    );

    assertEquals(List.of(), warnings);
  }
}
