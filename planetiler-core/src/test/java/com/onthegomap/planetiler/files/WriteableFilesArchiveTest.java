package com.onthegomap.planetiler.files;

import static org.junit.jupiter.api.Assertions.*;

import com.onthegomap.planetiler.TestUtils;
import com.onthegomap.planetiler.archive.TileEncodingResult;
import com.onthegomap.planetiler.config.Arguments;
import com.onthegomap.planetiler.geo.TileCoord;
import com.onthegomap.planetiler.geo.TileOrder;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class WriteableFilesArchiveTest {

  @Test
  void testWrite(@TempDir Path tempDir) throws IOException {
    final Path outputPath = tempDir.resolve("tiles");
    try (var archive = WriteableFilesArchive.newWriter(outputPath, Arguments.of(), false)) {
      archive.initialize();
      try (var tileWriter = archive.newTileWriter()) {
        tileWriter.write(new TileEncodingResult(TileCoord.ofXYZ(0, 0, 0), new byte[]{0}, OptionalLong.empty()));
        tileWriter.write(new TileEncodingResult(TileCoord.ofXYZ(1, 2, 3), new byte[]{1}, OptionalLong.of(1)));
        tileWriter.write(new TileEncodingResult(TileCoord.ofXYZ(1, 3, 3), new byte[]{2}, OptionalLong.of(2)));
        tileWriter.write(new TileEncodingResult(TileCoord.ofXYZ(1, 3, 4), new byte[]{3}, OptionalLong.of(3)));
      }
      archive.finish(TestUtils.MAX_METADATA_DESERIALIZED);
    }

    try (Stream<Path> s = Files.find(outputPath, 100, (p, attrs) -> attrs.isRegularFile())) {
      final List<Path> filesInDir = s.sorted().toList();
      assertEquals(
        List.of(
          Paths.get("0", "0", "0.pbf"),
          Paths.get("3", "1", "2.pbf"),
          Paths.get("3", "1", "3.pbf"),
          Paths.get("4", "1", "3.pbf"),
          Paths.get("metadata.json")
        ),
        filesInDir.stream().map(outputPath::relativize).toList()
      );
      assertArrayEquals(new byte[]{0}, Files.readAllBytes(filesInDir.get(0)));
      assertArrayEquals(new byte[]{1}, Files.readAllBytes(filesInDir.get(1)));
      assertArrayEquals(new byte[]{2}, Files.readAllBytes(filesInDir.get(2)));
      assertArrayEquals(new byte[]{3}, Files.readAllBytes(filesInDir.get(3)));
      TestUtils.assertSameJson(
        TestUtils.MAX_METADATA_SERIALIZED,
        Files.readString(filesInDir.get(4))
      );
    }
  }

  private void testMetadataWrite(Arguments options, Path archiveOutput, Path metadataOutputPath) throws IOException {
    try (var archive = WriteableFilesArchive.newWriter(archiveOutput, options, false)) {
      archive.initialize();
      archive.finish(TestUtils.MAX_METADATA_DESERIALIZED);
    }

    assertTrue(Files.exists(metadataOutputPath));
    TestUtils.assertSameJson(
      TestUtils.MAX_METADATA_SERIALIZED,
      Files.readString(metadataOutputPath)
    );
  }

  @Test
  void testMetadataWriteDefault(@TempDir Path tempDir) throws IOException {
    final Path outputPath = tempDir.resolve("tiles");
    testMetadataWrite(Arguments.of(), outputPath, outputPath.resolve("metadata.json"));
  }

  @Test
  void testMetadataWriteRelative(@TempDir Path tempDir) throws IOException {
    final Path outputPath = tempDir.resolve("tiles");
    final Arguments options = Arguments.of(Map.of(FilesArchiveUtils.OPTION_METADATA_PATH, "x.y"));
    testMetadataWrite(options, outputPath, outputPath.resolve("x.y"));
  }

  @Test
  void testMetadataWriteAbsolute(@TempDir Path tempDir) throws IOException {
    final Path outputPath = tempDir.resolve("tiles");
    final Path p = Files.createDirectory(tempDir.resolve("abs")).toAbsolutePath().resolve("abc.json");
    final Arguments options = Arguments.of(Map.of(FilesArchiveUtils.OPTION_METADATA_PATH, p.toString()));
    testMetadataWrite(options, outputPath, p);
  }

  @Test
  void testMetadataWriteNone(@TempDir Path tempDir) throws IOException {
    final Path outputPath = tempDir.resolve("tiles");
    final Arguments options = Arguments.of(Map.of(FilesArchiveUtils.OPTION_METADATA_PATH, "none"));
    try (var archive = WriteableFilesArchive.newWriter(outputPath, options, false)) {
      archive.initialize();
      archive.finish(TestUtils.MAX_METADATA_DESERIALIZED);
    }
    try (Stream<Path> ps = Files.find(outputPath, 100, (p, a) -> a.isRegularFile())) {
      assertEquals(List.of(), ps.toList());
    }
  }

  @Test
  void testMetadataFailsIfNotFile(@TempDir Path tempDir) throws IOException {
    final Path outputPath = tempDir.resolve("tiles");
    final Arguments options = Arguments.of(Map.of(FilesArchiveUtils.OPTION_METADATA_PATH, outputPath.toString()));
    try (var archive = WriteableFilesArchive.newWriter(outputPath, options, false)) {
      fail();
    } catch (IllegalArgumentException e) {
      // expected
    }
  }

  @Test
  void testMetadataOverwriteOff(@TempDir Path tempDir) throws IOException {
    final Path outputPath = tempDir.resolve("tiles");
    Files.createDirectory(outputPath);
    Files.writeString(outputPath.resolve("metadata.json"), "something");
    try (var archive = WriteableFilesArchive.newWriter(outputPath, Arguments.of(), false)) {
      fail();
    } catch (IllegalArgumentException e) {
      // expected
    }
  }

  @Test
  void testMetadataOverwriteOn(@TempDir Path tempDir) throws IOException {
    final Path outputPath = tempDir.resolve("tiles");
    final Path metadataPath = outputPath.resolve("metadata.json");
    Files.createDirectory(outputPath);
    Files.writeString(metadataPath, "something");
    try (var archive = WriteableFilesArchive.newWriter(outputPath, Arguments.of(), true)) {
      archive.initialize();
      archive.finish(TestUtils.MAX_METADATA_DESERIALIZED);
    }
    TestUtils.assertSameJson(
      TestUtils.MAX_METADATA_SERIALIZED,
      Files.readString(metadataPath)
    );
  }

  @Test
  void testCreatesPathIfNotExists(@TempDir Path tempDir) throws IOException {
    final Path outputPath = tempDir.resolve("tiles");
    try (var archive = WriteableFilesArchive.newWriter(outputPath, Arguments.of(), false)) {
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
    assertThrows(
      IllegalArgumentException.class,
      () -> WriteableFilesArchive.newWriter(outputPath, Arguments.of(), false)
    );
  }

  @Test
  void testFailsIfTileExistsAsDir(@TempDir Path tempDir) throws IOException {
    final Path outputPath = tempDir.resolve("tiles");
    final Path tileAsDirPath = outputPath.resolve(Paths.get("0", "0", "0.pbf"));
    Files.createDirectories(tileAsDirPath);
    try (var archive = WriteableFilesArchive.newWriter(outputPath, Arguments.of(), false)) {
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
    try (var archive = WriteableFilesArchive.newWriter(outputPath, Arguments.of(), false)) {
      try (var writer = archive.newTileWriter()) {
        final var r = new TileEncodingResult(TileCoord.ofXYZ(0, 0, 0), new byte[]{0}, OptionalLong.empty());
        assertThrows(IllegalStateException.class, () -> writer.write(r));
      }
    }
  }

  @Test
  void testSettings(@TempDir Path tempDir) throws IOException {
    final Path outputPath = tempDir.resolve("tiles");
    Files.createDirectories(outputPath);
    try (var archive = WriteableFilesArchive.newWriter(outputPath, Arguments.of(), false)) {
      assertFalse(archive.deduplicates());
      assertEquals(TileOrder.TMS, archive.tileOrder());

    }
  }
}
