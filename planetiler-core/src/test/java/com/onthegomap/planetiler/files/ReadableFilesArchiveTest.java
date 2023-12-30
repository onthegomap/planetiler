package com.onthegomap.planetiler.files;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class ReadableFilesArchiveTest {

  @Test
  void testRead(@TempDir Path tempDir) throws IOException {

    final Path tilesDir = tempDir.resolve("tiles");

    final List<Path> files = List.of(
      tilesDir.resolve(Paths.get("0", "0", "0.pbf")),
      tilesDir.resolve(Paths.get("1", "2", "3.pbf")),
      // invalid
      tilesDir.resolve(Paths.get("9", "9")),
      tilesDir.resolve(Paths.get("9", "x")),
      tilesDir.resolve(Paths.get("9", "8", "9")),
      tilesDir.resolve(Paths.get("9", "8", "9.")),
      tilesDir.resolve(Paths.get("9", "8", "x.pbf")),
      tilesDir.resolve(Paths.get("9", "b", "1.pbf")),
      tilesDir.resolve(Paths.get("a", "8", "1.pbf")),
      tilesDir.resolve(Paths.get("9", "7.pbf")),
      tilesDir.resolve(Paths.get("8.pbf"))
    );
    for (int i = 0; i < files.size(); i++) {
      final Path file = files.get(i);
      Files.createDirectories(file.getParent());
      Files.write(files.get(i), new byte[]{(byte) i});
    }

    try (var reader = ReadableFilesArchive.newReader(tilesDir, Arguments.of())) {
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
    final Path tilesDir = tempDir.resolve("tiles");
    Files.createDirectories(tilesDir);
    try (var reader = ReadableFilesArchive.newReader(tilesDir, Arguments.of())) {
      assertNull(reader.getTile(0, 0, 0));
    }
  }

  @Test
  void testFailsToReadTileFromDir(@TempDir Path tempDir) throws IOException {
    final Path tilesDir = tempDir.resolve("tiles");
    Files.createDirectories(tilesDir.resolve(Paths.get("0", "0", "0.pbf")));
    try (var reader = ReadableFilesArchive.newReader(tilesDir, Arguments.of())) {
      assertThrows(UncheckedIOException.class, () -> reader.getTile(0, 0, 0));
    }
  }

  @Test
  void testRequiresExistingPath(@TempDir Path tempDir) {
    final Path tilesDir = tempDir.resolve("tiles");
    final Arguments options = Arguments.of();
    assertThrows(IllegalArgumentException.class, () -> ReadableFilesArchive.newReader(tilesDir, options));
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
  void testReadCustomScheme(String tileScheme, Path tileFile, @TempDir Path tempDir) throws IOException {
    final Path tilesDir = tempDir.resolve("tiles");
    tileFile = tilesDir.resolve(tileFile);
    Files.createDirectories(tileFile.getParent());
    Files.write(tileFile, new byte[]{1});

    final Arguments options = Arguments.of(Map.of(FilesArchiveUtils.OPTION_TILE_SCHEME, tileScheme));
    try (var archive = ReadableFilesArchive.newReader(tilesDir, options)) {
      assertEquals(
        List.of(TileCoord.ofXYZ(1, 2, 3)),
        archive.getAllTileCoords().stream().toList()
      );
    }
  }

  @Test
  void testTileSchemeFromBasePath(@TempDir Path tempDir) throws IOException {
    final Path tilesDir = tempDir.resolve("tiles");
    final Path basePath = tilesDir.resolve(Paths.get("{x}", "{y}", "{z}.pbf"));
    final Path tileFile = tilesDir.resolve(Paths.get("1", "2", "3.pbf"));
    Files.createDirectories(tileFile.getParent());
    Files.write(tileFile, new byte[]{1});

    final Path metadataFile = tilesDir.resolve("metadata.json");
    Files.writeString(metadataFile, TestUtils.MAX_METADATA_SERIALIZED);

    try (var archive = ReadableFilesArchive.newReader(basePath, Arguments.of())) {
      assertEquals(
        List.of(TileCoord.ofXYZ(1, 2, 3)),
        archive.getAllTileCoords().stream().toList()
      );
      assertNotNull(archive.metadata());
    }
  }

  @Test
  void testHasNoMetaData(@TempDir Path tempDir) throws IOException {
    final Path tilesDir = tempDir.resolve("tiles");
    Files.createDirectories(tilesDir);
    try (var reader = ReadableFilesArchive.newReader(tilesDir, Arguments.of())) {
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
    final Path tilesDir = Files.createDirectories(tempDir.resolve("tiles"));
    testMetadata(tilesDir, Arguments.of(), tilesDir.resolve("metadata.json"));
  }

  @Test
  void testMetadataRelative(@TempDir Path tempDir) throws IOException {
    final Path tilesDir = Files.createDirectories(tempDir.resolve("tiles"));
    final Path meteadataPath = tilesDir.resolve("x.y");
    final Arguments options = Arguments.of(Map.of(FilesArchiveUtils.OPTION_METADATA_PATH, "x.y"));
    testMetadata(tilesDir, options, meteadataPath);
  }

  @Test
  void testMetadataAbsolute(@TempDir Path tempDir) throws IOException {
    final Path tilesDir = Files.createDirectories(tempDir.resolve("tiles"));
    final Path meteadataPath = Files.createDirectories(tempDir.resolve(Paths.get("abs"))).resolve("x.y");
    final Arguments options =
      Arguments.of(Map.of(FilesArchiveUtils.OPTION_METADATA_PATH, meteadataPath.toAbsolutePath().toString()));
    testMetadata(tilesDir, options, meteadataPath);
  }

  @Test
  void testMetadataNone(@TempDir Path tempDir) throws IOException {
    final Path tilesDir = Files.createDirectories(tempDir.resolve("tiles"));
    final Path meteadataPath = tilesDir.resolve("none");
    final Arguments options = Arguments.of(Map.of(FilesArchiveUtils.OPTION_METADATA_PATH, "none"));

    try (var reader = ReadableFilesArchive.newReader(tilesDir, options)) {
      assertNull(reader.metadata());

      Files.writeString(meteadataPath, TestUtils.MAX_METADATA_SERIALIZED);
      assertNull(reader.metadata());
    }
  }
}
