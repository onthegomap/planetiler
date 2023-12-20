package com.onthegomap.planetiler.files;

import static org.junit.jupiter.api.Assertions.*;

import com.onthegomap.planetiler.archive.TileEncodingResult;
import com.onthegomap.planetiler.geo.TileCoord;
import com.onthegomap.planetiler.geo.TileOrder;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.OptionalLong;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class WriteableFilesArchiveTest {

  @Test
  void testWrite(@TempDir Path tempDir) throws IOException {
    final Path outputPath = tempDir.resolve("tiles");
    try (var archive = WriteableFilesArchive.newWriter(outputPath)) {
      try (var tileWriter = archive.newTileWriter()) {
        tileWriter.write(new TileEncodingResult(TileCoord.ofXYZ(0, 0, 0), new byte[]{0}, OptionalLong.empty()));
        tileWriter.write(new TileEncodingResult(TileCoord.ofXYZ(1, 2, 3), new byte[]{1}, OptionalLong.of(1)));
        tileWriter.write(new TileEncodingResult(TileCoord.ofXYZ(1, 3, 3), new byte[]{2}, OptionalLong.of(2)));
        tileWriter.write(new TileEncodingResult(TileCoord.ofXYZ(1, 3, 4), new byte[]{3}, OptionalLong.of(3)));
      }
    }

    try (Stream<Path> s = Files.find(outputPath, 100, (p, attrs) -> attrs.isRegularFile())) {
      final List<Path> filesInDir = s.sorted().toList();
      assertEquals(
        List.of(
          Paths.get("0", "0", "0.pbf"),
          Paths.get("3", "1", "2.pbf"),
          Paths.get("3", "1", "3.pbf"),
          Paths.get("4", "1", "3.pbf")
        ),
        filesInDir.stream().map(outputPath::relativize).toList()
      );
      assertArrayEquals(new byte[]{0}, Files.readAllBytes(filesInDir.get(0)));
      assertArrayEquals(new byte[]{1}, Files.readAllBytes(filesInDir.get(1)));
      assertArrayEquals(new byte[]{2}, Files.readAllBytes(filesInDir.get(2)));
      assertArrayEquals(new byte[]{3}, Files.readAllBytes(filesInDir.get(3)));
    }
  }

  @Test
  void testCreatesPathIfNotExists(@TempDir Path tempDir) throws IOException {
    final Path outputPath = tempDir.resolve("tiles");
    try (var archive = WriteableFilesArchive.newWriter(outputPath)) {
      try (var writer = archive.newTileWriter()) {
        writer.write(new TileEncodingResult(TileCoord.ofXYZ(0, 0, 0), new byte[]{0}, OptionalLong.empty()));
      }
    }
    assertTrue(Files.isRegularFile(outputPath.resolve(Paths.get("0", "0", "0.pbf"))));
  }

  @Test
  void testFailsIfBasePathIsNoDirectory(@TempDir Path tempDir) throws IOException {
    final Path outputPath = tempDir.resolve("tiles");
    Files.createFile(outputPath);
    assertThrows(IllegalArgumentException.class, () -> WriteableFilesArchive.newWriter(outputPath));
  }

  @Test
  void testFailsIfTileExistsAsDir(@TempDir Path tempDir) throws IOException {
    final Path outputPath = tempDir.resolve("tiles");
    final Path tileAsDirPath = outputPath.resolve(Paths.get("0", "0", "0.pbf"));
    Files.createDirectories(tileAsDirPath);
    try (var archive = WriteableFilesArchive.newWriter(outputPath)) {
      try (var writer = archive.newTileWriter()) {
        final var r = new TileEncodingResult(TileCoord.ofXYZ(0, 0, 0), new byte[]{0}, OptionalLong.empty());
        assertThrows(UncheckedIOException.class, () -> writer.write(r));
      }
    }
  }

  @Test
  void testFailsIfDirExistsAsFile(@TempDir Path tempDir) throws IOException {
    final Path outputPath = tempDir.resolve("tiles");
    Files.createDirectories(outputPath);
    Files.createFile(outputPath.resolve("0"));
    try (var archive = WriteableFilesArchive.newWriter(outputPath)) {
      try (var writer = archive.newTileWriter()) {
        final var r = new TileEncodingResult(TileCoord.ofXYZ(0, 0, 0), new byte[]{0}, OptionalLong.empty());
        assertThrows(UncheckedIOException.class, () -> writer.write(r));
      }
    }
  }

  @Test
  void testSettings(@TempDir Path tempDir) throws IOException {
    final Path outputPath = tempDir.resolve("tiles");
    Files.createDirectories(outputPath);
    try (var archive = WriteableFilesArchive.newWriter(outputPath)) {
      assertFalse(archive.deduplicates());
      assertEquals(TileOrder.TMS, archive.tileOrder());

    }
  }
}
