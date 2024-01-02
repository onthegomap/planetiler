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
import java.util.Objects;
import java.util.OptionalLong;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class WriteableFilesArchiveTest {

  @Test
  void testWrite(@TempDir Path tempDir) throws IOException {
    final Path tilesDir = tempDir.resolve("tiles");
    try (var archive = WriteableFilesArchive.newWriter(tilesDir, Arguments.of(), false)) {
      archive.initialize();
      try (var tileWriter = archive.newTileWriter()) {
        tileWriter.write(new TileEncodingResult(TileCoord.ofXYZ(0, 0, 0), new byte[]{0}, OptionalLong.empty()));
        tileWriter.write(new TileEncodingResult(TileCoord.ofXYZ(1, 2, 3), new byte[]{1}, OptionalLong.of(1)));
        tileWriter.write(new TileEncodingResult(TileCoord.ofXYZ(1, 3, 3), new byte[]{2}, OptionalLong.of(2)));
        tileWriter.write(new TileEncodingResult(TileCoord.ofXYZ(1, 3, 4), new byte[]{3}, OptionalLong.of(3)));
      }
      archive.finish(TestUtils.MAX_METADATA_DESERIALIZED);
    }

    try (Stream<Path> s = Files.find(tilesDir, 100, (p, attrs) -> attrs.isRegularFile())) {
      final List<Path> filesInDir = s.sorted().toList();
      assertEquals(
        List.of(
          Paths.get("0", "0", "0.pbf"),
          Paths.get("3", "1", "2.pbf"),
          Paths.get("3", "1", "3.pbf"),
          Paths.get("4", "1", "3.pbf"),
          Paths.get("metadata.json")
        ),
        filesInDir.stream().map(tilesDir::relativize).toList()
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

  @ParameterizedTest
  @CsvSource(textBlock = """
    {z}/{x}/{y}.pbf,    3/1/2.pbf
    {x}/{y}/{z}.pbf,    1/2/3.pbf
    {x}-{y}-{z}.pbf,    1-2-3.pbf
    {x}/a/{y}/b{z}.pbf, 1/a/2/b3.pbf
    {z}/{x}/{y}.pbf.gz, 3/1/2.pbf.gz
    {z}/{xs}/{ys}.pbf,  3/000/001/000/002.pbf
    {z}/{x}/{ys}.pbf,   3/1/000/002.pbf
    {z}/{xs}/{y}.pbf,   3/000/001/2.pbf
    """
  )
  void testWriteCustomScheme(String tileScheme, Path expectedFile, @TempDir Path tempDir) throws IOException {
    final Path tilesDir = tempDir.resolve("tiles");
    expectedFile = tilesDir.resolve(expectedFile);
    final Arguments options = Arguments.of(Map.of(FilesArchiveUtils.OPTION_TILE_SCHEME, tileScheme));
    try (var archive = WriteableFilesArchive.newWriter(tilesDir, options, false)) {
      try (var tileWriter = archive.newTileWriter()) {
        tileWriter.write(new TileEncodingResult(TileCoord.ofXYZ(1, 2, 3), new byte[]{1}, OptionalLong.empty()));
      }
    }
    assertTrue(Files.exists(expectedFile));
  }

  @ParameterizedTest
  @CsvSource(textBlock = """
    {z}/{x}/{y}.pbf           ,      , 3/1/2.pbf
    tiles/{z}/{x}/{y}.pbf     , tiles, tiles/3/1/2.pbf
    tiles/z{z}/{x}/{y}.pbf    , tiles, tiles/z3/1/2.pbf
    z{z}/x{x}/y{y}.pbf        ,      , z3/x1/y2.pbf
    tiles/tile-{z}-{x}-{y}.pbf, tiles, tiles/tile-3-1-2.pbf
    """
  )
  void testTileSchemeFromBasePath(Path shortcutBasePath, Path actualBasePath, Path tileFile, @TempDir Path tempDir)
    throws IOException {
    final Path testBase = tempDir.resolve("tiles");

    shortcutBasePath = testBase.resolve(shortcutBasePath);
    actualBasePath = testBase.resolve(Objects.requireNonNullElse(actualBasePath, Paths.get("")));
    tileFile = testBase.resolve(tileFile);

    try (var archive = WriteableFilesArchive.newWriter(shortcutBasePath, Arguments.of(), false)) {
      try (var tileWriter = archive.newTileWriter()) {
        tileWriter.write(new TileEncodingResult(TileCoord.ofXYZ(1, 2, 3), new byte[]{1}, OptionalLong.empty()));
      }
      archive.finish(TestUtils.MAX_METADATA_DESERIALIZED);
    }

    assertTrue(Files.exists(tileFile));
    assertTrue(Files.exists(actualBasePath.resolve("metadata.json")));
  }

  private void testMetadataWrite(Arguments options, Path archiveOutput, Path metadataTilesDir) throws IOException {
    try (var archive = WriteableFilesArchive.newWriter(archiveOutput, options, false)) {
      archive.initialize();
      archive.finish(TestUtils.MAX_METADATA_DESERIALIZED);
    }

    assertTrue(Files.exists(metadataTilesDir));
    TestUtils.assertSameJson(
      TestUtils.MAX_METADATA_SERIALIZED,
      Files.readString(metadataTilesDir)
    );
  }

  @Test
  void testMetadataWriteDefault(@TempDir Path tempDir) throws IOException {
    final Path tilesDir = tempDir.resolve("tiles");
    testMetadataWrite(Arguments.of(), tilesDir, tilesDir.resolve("metadata.json"));
  }

  @Test
  void testMetadataWriteRelative(@TempDir Path tempDir) throws IOException {
    final Path tilesDir = tempDir.resolve("tiles");
    final Arguments options = Arguments.of(Map.of(FilesArchiveUtils.OPTION_METADATA_PATH, "x.y"));
    testMetadataWrite(options, tilesDir, tilesDir.resolve("x.y"));
  }

  @Test
  void testMetadataWriteAbsolute(@TempDir Path tempDir) throws IOException {
    final Path tilesDir = tempDir.resolve("tiles");
    final Path p = Files.createDirectory(tempDir.resolve("abs")).toAbsolutePath().resolve("abc.json");
    final Arguments options = Arguments.of(Map.of(FilesArchiveUtils.OPTION_METADATA_PATH, p.toString()));
    testMetadataWrite(options, tilesDir, p);
  }

  @Test
  void testMetadataWriteNone(@TempDir Path tempDir) throws IOException {
    final Path tilesDir = tempDir.resolve("tiles");
    final Arguments options = Arguments.of(Map.of(FilesArchiveUtils.OPTION_METADATA_PATH, "none"));
    try (var archive = WriteableFilesArchive.newWriter(tilesDir, options, false)) {
      archive.initialize();
      archive.finish(TestUtils.MAX_METADATA_DESERIALIZED);
    }
    try (Stream<Path> ps = Files.find(tilesDir, 100, (p, a) -> a.isRegularFile())) {
      assertEquals(List.of(), ps.toList());
    }
  }

  @Test
  void testMetadataFailsIfNotFile(@TempDir Path tempDir) throws IOException {
    final Path tilesDir = tempDir.resolve("tiles");
    final Arguments options = Arguments.of(Map.of(FilesArchiveUtils.OPTION_METADATA_PATH, tilesDir.toString()));
    try (var archive = WriteableFilesArchive.newWriter(tilesDir, options, false)) {
      fail();
    } catch (IllegalArgumentException e) {
      // expected
    }
  }

  @Test
  void testMetadataOverwriteOff(@TempDir Path tempDir) throws IOException {
    final Path tilesDir = tempDir.resolve("tiles");
    Files.createDirectory(tilesDir);
    Files.writeString(tilesDir.resolve("metadata.json"), "something");
    try (var archive = WriteableFilesArchive.newWriter(tilesDir, Arguments.of(), false)) {
      fail();
    } catch (IllegalArgumentException e) {
      // expected
    }
  }

  @Test
  void testMetadataOverwriteOn(@TempDir Path tempDir) throws IOException {
    final Path tilesDir = tempDir.resolve("tiles");
    final Path metadataPath = tilesDir.resolve("metadata.json");
    Files.createDirectory(tilesDir);
    Files.writeString(metadataPath, "something");
    try (var archive = WriteableFilesArchive.newWriter(tilesDir, Arguments.of(), true)) {
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
    final Path tilesDir = tempDir.resolve("tiles");
    try (var archive = WriteableFilesArchive.newWriter(tilesDir, Arguments.of(), false)) {
      try (var writer = archive.newTileWriter()) {
        writer.write(new TileEncodingResult(TileCoord.ofXYZ(0, 0, 0), new byte[]{0}, OptionalLong.empty()));
      }
    }
    assertTrue(Files.isRegularFile(tilesDir.resolve(Paths.get("0", "0", "0.pbf"))));
  }

  @Test
  void testFailsIfBasePathIsNoDirectory(@TempDir Path tempDir) throws IOException {
    final Path tilesDir = tempDir.resolve("tiles");
    Files.createFile(tilesDir);
    final Arguments options = Arguments.of();
    assertThrows(
      IllegalArgumentException.class,
      () -> WriteableFilesArchive.newWriter(tilesDir, options, false)
    );
  }

  @Test
  void testFailsIfTileExistsAsDir(@TempDir Path tempDir) throws IOException {
    final Path tilesDir = tempDir.resolve("tiles");
    final Path tileAsDirPath = tilesDir.resolve(Paths.get("0", "0", "0.pbf"));
    Files.createDirectories(tileAsDirPath);
    try (var archive = WriteableFilesArchive.newWriter(tilesDir, Arguments.of(), false)) {
      try (var writer = archive.newTileWriter()) {
        final var r = new TileEncodingResult(TileCoord.ofXYZ(0, 0, 0), new byte[]{0}, OptionalLong.empty());
        assertThrows(UncheckedIOException.class, () -> writer.write(r));
      }
    }
  }

  @Test
  void testFailsIfDirExistsAsFile(@TempDir Path tempDir) throws IOException {
    final Path tilesDir = tempDir.resolve("tiles");
    Files.createDirectories(tilesDir);
    Files.createFile(tilesDir.resolve("0"));
    try (var archive = WriteableFilesArchive.newWriter(tilesDir, Arguments.of(), false)) {
      try (var writer = archive.newTileWriter()) {
        final var r = new TileEncodingResult(TileCoord.ofXYZ(0, 0, 0), new byte[]{0}, OptionalLong.empty());
        assertThrows(IllegalStateException.class, () -> writer.write(r));
      }
    }
  }

  @Test
  void testSettings(@TempDir Path tempDir) throws IOException {
    final Path tilesDir = tempDir.resolve("tiles");
    Files.createDirectories(tilesDir);
    try (var archive = WriteableFilesArchive.newWriter(tilesDir, Arguments.of(), false)) {
      assertFalse(archive.deduplicates());
      assertEquals(TileOrder.TMS, archive.tileOrder());

    }
  }
}
