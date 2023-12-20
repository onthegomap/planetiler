package com.onthegomap.planetiler.archive;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

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

  @ParameterizedTest
  @EnumSource(TileArchiveConfig.Format.class)
  void testByFormatParam(TileArchiveConfig.Format format) {
    final var config = TileArchiveConfig.from("output?format=" + format.id());
    assertEquals(format, config.format());
    assertEquals(TileArchiveConfig.Scheme.FILE, config.scheme());
    assertEquals(Path.of("output").toAbsolutePath(), config.getLocalPath());
    assertEquals(Map.of("format", format.id()), config.options());
  }

  @ParameterizedTest
  @EnumSource(TileArchiveConfig.Format.class)
  void testGetPathForMultiThreadedWriter(TileArchiveConfig.Format format) {
    final var config = TileArchiveConfig.from("output?format=" + format.id());
    if (!format.supportsConcurrentWrites()) {
      assertThrows(UnsupportedOperationException.class, () -> config.getPathForMultiThreadedWriter(0));
      assertThrows(UnsupportedOperationException.class, () -> config.getPathForMultiThreadedWriter(1));
    } else {
      assertEquals(config.getLocalPath(), config.getPathForMultiThreadedWriter(0));
      final Path p = config.getPathForMultiThreadedWriter(1);
      switch (format) {
        case FILES -> assertEquals(p, config.getLocalPath());
        default -> assertEquals(config.getLocalPath().getParent().resolve(Paths.get("output1")), p);
      }
    }
  }

  @Test
  void testExistsForFilesArchive(@TempDir Path tempDir) throws IOException {
    final Path out = tempDir.resolve("outdir");
    final var config = TileArchiveConfig.from(out + "?format=files");
    assertFalse(config.exists());
    Files.createDirectory(out);
    assertFalse(config.exists());
    Files.createFile(out.resolve("1"));
    assertTrue(config.exists());
  }

  @Test
  void testExistsForNonFilesArchive(@TempDir Path tempDir) throws IOException {
    final Path mbtilesOut = tempDir.resolve("out.mbtiles");
    final var config = TileArchiveConfig.from(mbtilesOut.toString());
    assertFalse(config.exists());
    Files.createFile(mbtilesOut);
    assertTrue(config.exists());
  }
}
