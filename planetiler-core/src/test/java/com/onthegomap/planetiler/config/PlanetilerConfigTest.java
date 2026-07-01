package com.onthegomap.planetiler.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class PlanetilerConfigTest {

  @Test
  void testTileExtentMustBePowerOf2() {
    var exception = assertThrows(IllegalArgumentException.class,
      () -> PlanetilerConfig.from(Arguments.of("tile_extent", "5000")));
    assertTrue(exception.getMessage().contains("power of 2"));
  }

  @Test
  void testTileExtentPowerOf2Allowed() {
    assertEquals(8192, PlanetilerConfig.from(Arguments.of("tile_extent", "8192")).tileExtent());
  }
}
