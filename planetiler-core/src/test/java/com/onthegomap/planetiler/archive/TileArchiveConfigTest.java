package com.onthegomap.planetiler.archive;

import static org.junit.jupiter.api.Assertions.assertEquals;

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
}
