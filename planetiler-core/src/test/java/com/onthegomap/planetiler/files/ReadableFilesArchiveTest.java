package com.onthegomap.planetiler.files;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.onthegomap.planetiler.TestUtils;
import com.onthegomap.planetiler.archive.Tile;
import com.onthegomap.planetiler.config.Arguments;
import com.onthegomap.planetiler.geo.TileCoord;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class ReadableFilesArchiveTest {

  @Test
  void testRead(@TempDir Path tempDir) throws IOException {

    final Path outputPath = tempDir.resolve("tiles");

    final List<Path> files = List.of(
      outputPath.resolve(Paths.get("0", "0", "0.pbf")),
      outputPath.resolve(Paths.get("1", "2", "3.pbf")),
      // invalid
      outputPath.resolve(Paths.get("9", "9")),
      outputPath.resolve(Paths.get("9", "x")),
      outputPath.resolve(Paths.get("9", "8", "9")),
      outputPath.resolve(Paths.get("9", "8", "9.")),
      outputPath.resolve(Paths.get("9", "8", "x.pbf")),
      outputPath.resolve(Paths.get("9", "b", "1.pbf")),
      outputPath.resolve(Paths.get("a", "8", "1.pbf")),
      outputPath.resolve(Paths.get("9", "7.pbf")),
      outputPath.resolve(Paths.get("8.pbf"))
    );
    for (int i = 0; i < files.size(); i++) {
      final Path file = files.get(i);
      Files.createDirectories(file.getParent());
      Files.write(files.get(i), new byte[]{(byte) i});
    }

    try (var reader = ReadableFilesArchive.newReader(outputPath, Arguments.of())) {
      final List<Tile> tiles = reader.getAllTiles().stream().sorted().toList();
      assertEquals(
        List.of(
          new Tile(TileCoord.ofXYZ(0, 0, 0), new byte[]{0}),
          new Tile(TileCoord.ofXYZ(2, 3, 1), new byte[]{1})
        ),
        tiles
      );
    }
  }

  @Test
  void testGetTileNotExists(@TempDir Path tempDir) throws IOException {
    final Path outputPath = tempDir.resolve("tiles");
    Files.createDirectories(outputPath);
    try (var reader = ReadableFilesArchive.newReader(outputPath, Arguments.of())) {
      assertNull(reader.getTile(0, 0, 0));
    }
  }

  @Test
  void testFailsToReadTileFromDir(@TempDir Path tempDir) throws IOException {
    final Path outputPath = tempDir.resolve("tiles");
    Files.createDirectories(outputPath.resolve(Paths.get("0", "0", "0.pbf")));
    try (var reader = ReadableFilesArchive.newReader(outputPath, Arguments.of())) {
      assertThrows(UncheckedIOException.class, () -> reader.getTile(0, 0, 0));
    }
  }

  @Test
  void testRequiresExistingPath(@TempDir Path tempDir) {
    final Path outputPath = tempDir.resolve("tiles");
    assertThrows(IllegalArgumentException.class, () -> ReadableFilesArchive.newReader(outputPath, Arguments.of()));
  }

  @Test
  void testHasNoMetaData(@TempDir Path tempDir) throws IOException {
    final Path outputPath = tempDir.resolve("tiles");
    Files.createDirectories(outputPath);
    try (var reader = ReadableFilesArchive.newReader(outputPath, Arguments.of())) {
      assertNull(reader.metadata());
    }
  }

  private void testMetadata(Path basePath, Arguments options, Path metadataPath) throws IOException {
    try (var reader = ReadableFilesArchive.newReader(basePath, options)) {
      assertNull(reader.metadata());

      Files.writeString(metadataPath, TestUtils.MAX_METADATA_SERIALIZED);
      assertEquals(TestUtils.MAX_METADATA_DESERIALIZED, reader.metadata());
    }
  }

  @Test
  void testMetadataDefault(@TempDir Path tempDir) throws IOException {
    final Path outputPath = Files.createDirectories(tempDir.resolve("tiles"));
    testMetadata(outputPath, Arguments.of(), outputPath.resolve("metadata.json"));
  }

  @Test
  void testMetadataRelative(@TempDir Path tempDir) throws IOException {
    final Path outputPath = Files.createDirectories(tempDir.resolve("tiles"));
    final Path meteadataPath = outputPath.resolve("x.y");
    final Arguments options = Arguments.of(Map.of(FilesArchiveUtils.OPTION_METADATA_PATH, "x.y"));
    testMetadata(outputPath, options, meteadataPath);
  }

  @Test
  void testMetadataAbsolute(@TempDir Path tempDir) throws IOException {
    final Path outputPath = Files.createDirectories(tempDir.resolve("tiles"));
    final Path meteadataPath = Files.createDirectories(tempDir.resolve(Paths.get("abs"))).resolve("x.y");
    final Arguments options =
      Arguments.of(Map.of(FilesArchiveUtils.OPTION_METADATA_PATH, meteadataPath.toAbsolutePath().toString()));
    testMetadata(outputPath, options, meteadataPath);
  }

  @Test
  void testMetadataNone(@TempDir Path tempDir) throws IOException {
    final Path outputPath = Files.createDirectories(tempDir.resolve("tiles"));
    final Path meteadataPath = outputPath.resolve("none");
    final Arguments options = Arguments.of(Map.of(FilesArchiveUtils.OPTION_METADATA_PATH, "none"));

    try (var reader = ReadableFilesArchive.newReader(outputPath, options)) {
      assertNull(reader.metadata());

      Files.writeString(meteadataPath, TestUtils.MAX_METADATA_SERIALIZED);
      assertNull(reader.metadata());
    }
  }
}
