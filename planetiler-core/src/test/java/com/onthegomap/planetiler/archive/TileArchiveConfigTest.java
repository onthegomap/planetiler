package com.onthegomap.planetiler.archive;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.nio.file.Path;
import java.util.Map;
import org.junit.jupiter.api.Test;

class TileArchiveConfigTest {

  @Test
  void testMbtiles() {
    var config = TileArchiveConfig.from("output.mbtiles");
    assertEquals(TileArchiveConfig.Format.MBTILES, config.format());
    assertEquals(TileArchiveConfig.Scheme.FILE, config.scheme());
    assertEquals(Map.of(), config.options());
    assertEquals(Path.of("output.mbtiles").toAbsolutePath(), config.getLocalPath());
  }

  @Test
  void testMbtilesWithOptions() {
    var config = TileArchiveConfig.from("output.mbtiles?compact=true");
    assertEquals(TileArchiveConfig.Format.MBTILES, config.format());
    assertEquals(TileArchiveConfig.Scheme.FILE, config.scheme());
    assertEquals(Map.of("compact", "true"), config.options());
    assertEquals(Path.of("output.mbtiles").toAbsolutePath(), config.getLocalPath());
  }

  @Test
  void testPmtiles() {
    assertEquals(TileArchiveConfig.Format.PMTILES, TileArchiveConfig.from("output.pmtiles").format());
    assertEquals(TileArchiveConfig.Format.PMTILES, TileArchiveConfig.from("output.mbtiles?format=pmtiles").format());
    assertEquals(TileArchiveConfig.Format.PMTILES,
      TileArchiveConfig.from("file:///output.mbtiles?format=pmtiles").format());
  }

  @Test
  void testPgtiles() {
    var config = TileArchiveConfig.from("postgres://localhost:5432/db?user=user&password=password");
    assertEquals(TileArchiveConfig.Format.POSTGRES, config.format());
    assertEquals(Map.of("user", "user", "password", "password"), config.options());
    assertEquals("localhost:5432", config.uri().getAuthority());
    assertEquals("/db", config.uri().getPath());
    assertNull(config.getLocalPath());

    config = TileArchiveConfig.from("postgres://localhost:5432?user=user&password=password");
    assertEquals("", config.uri().getPath());
  }
}
