package com.onthegomap.planetiler.archive;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.file.Path;
import java.util.Map;
import org.junit.jupiter.api.Test;

class TileArchiveConfigTest {

  @Test
  void testMbtiles() {
    var config = TileArchive.from("output.mbtiles");
    assertEquals(TileArchive.Format.MBTILES, config.format());
    assertEquals(TileArchive.Scheme.FILE, config.scheme());
    assertEquals(Map.of(), config.options());
    assertEquals(Path.of("output.mbtiles").toAbsolutePath(), config.getLocalPath());
  }

  @Test
  void testMbtilesWithOptions() {
    var config = TileArchive.from("output.mbtiles?compact=true");
    assertEquals(TileArchive.Format.MBTILES, config.format());
    assertEquals(TileArchive.Scheme.FILE, config.scheme());
    assertEquals(Map.of("compact", "true"), config.options());
    assertEquals(Path.of("output.mbtiles").toAbsolutePath(), config.getLocalPath());
  }

  @Test
  void testPmtiles() {
    assertEquals(TileArchive.Format.PMTILES, TileArchive.from("output.pmtiles").format());
    assertEquals(TileArchive.Format.PMTILES, TileArchive.from("output.mbtiles?format=pmtiles").format());
    assertEquals(TileArchive.Format.PMTILES,
      TileArchive.from("file:///output.mbtiles?format=pmtiles").format());
  }
}
