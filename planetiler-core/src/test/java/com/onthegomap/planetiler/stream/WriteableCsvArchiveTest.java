package com.onthegomap.planetiler.stream;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.onthegomap.planetiler.archive.TileArchiveMetadata;
import com.onthegomap.planetiler.archive.TileEncodingResult;
import com.onthegomap.planetiler.config.Arguments;
import com.onthegomap.planetiler.geo.TileCoord;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class WriteableCsvArchiveTest {

  private static final StreamArchiveConfig defaultConfig = new StreamArchiveConfig(false, Arguments.of());
  private static final TileArchiveMetadata defaultMetadata =
    new TileArchiveMetadata("start", null, null, null, null, null, null, null, null, null, null, null, null);

  @Test
  void testWriteToSingleFile(@TempDir Path tempDir) throws IOException {

    final Path csvFile = tempDir.resolve("mbtiles.csv");

    try (var archive = WriteableCsvArchive.newWriteToFile(csvFile, defaultConfig)) {
      archive.initialize(defaultMetadata); // ignored
      try (var tileWriter = archive.newTileWriter()) {
        tileWriter.write(new TileEncodingResult(TileCoord.ofXYZ(0, 0, 0), new byte[]{0}, OptionalLong.empty()));
        tileWriter.write(new TileEncodingResult(TileCoord.ofXYZ(1, 2, 3), new byte[]{1}, OptionalLong.of(1)));
      }
      archive.finish(defaultMetadata);
    }
    assertEquals(
      """
        0,0,0,AA==
        1,2,3,AQ==
        """,
      Files.readString(csvFile)
    );

    assertEquals(Set.of(csvFile), Files.list(tempDir).collect(Collectors.toUnmodifiableSet()));
  }

  @Test
  void testWriteToMultipleFiles(@TempDir Path tempDir) throws IOException {

    final Path csvFilePrimary = tempDir.resolve("mbtiles.csv");
    final Path csvFileSecondary = tempDir.resolve("mbtiles.csv1");
    final Path csvFileTertiary = tempDir.resolve("mbtiles.csv2");

    try (var archive = WriteableCsvArchive.newWriteToFile(csvFilePrimary, defaultConfig)) {
      archive.initialize(defaultMetadata); // ignored
      try (var tileWriter = archive.newTileWriter()) {
        tileWriter.write(new TileEncodingResult(TileCoord.ofXYZ(11, 12, 1), new byte[]{0}, OptionalLong.empty()));
        tileWriter.write(new TileEncodingResult(TileCoord.ofXYZ(21, 22, 2), new byte[]{1}, OptionalLong.empty()));
      }
      try (var tileWriter = archive.newTileWriter()) {
        tileWriter.write(new TileEncodingResult(TileCoord.ofXYZ(31, 32, 3), new byte[]{2}, OptionalLong.empty()));
        tileWriter.write(new TileEncodingResult(TileCoord.ofXYZ(41, 42, 4), new byte[]{3}, OptionalLong.empty()));
      }
      try (var tileWriter = archive.newTileWriter()) {
        tileWriter.write(new TileEncodingResult(TileCoord.ofXYZ(51, 55, 5), new byte[]{4}, OptionalLong.empty()));
      }
      archive.finish(defaultMetadata);
    }

    assertEquals(
      """
        11,12,1,AA==
        21,22,2,AQ==
        """,
      Files.readString(csvFilePrimary)
    );
    assertEquals(
      """
        31,32,3,Ag==
        41,42,4,Aw==
        """,
      Files.readString(csvFileSecondary)
    );
    assertEquals(
      """
        51,55,5,BA==
        """,
      Files.readString(csvFileTertiary)
    );

    assertEquals(
      Set.of(csvFilePrimary, csvFileSecondary, csvFileTertiary),
      Files.list(tempDir).collect(Collectors.toUnmodifiableSet())
    );
  }

  @Test
  void testColumnSeparator(@TempDir Path tempDir) throws IOException {

    final StreamArchiveConfig config =
      new StreamArchiveConfig(false, Arguments.of(Map.of(WriteableCsvArchive.OPTION_COLUMN_SEPARATOR, "' '")));

    final String expectedCsv =
      """
        0,0,0,AAE=
        1,1,1,AgM=
        """.replace(',', ' ');

    testTileOptions(tempDir, config, expectedCsv);
  }

  @Test
  void testLineSeparator(@TempDir Path tempDir) throws IOException {

    final StreamArchiveConfig config =
      new StreamArchiveConfig(false, Arguments.of(Map.of(WriteableCsvArchive.OPTION_LINE_SEPARTATOR, "'\\r'")));

    final String expectedCsv =
      """
        0,0,0,AAE=
        1,1,1,AgM=
        """.replace('\n', '\r');

    testTileOptions(tempDir, config, expectedCsv);
  }

  @Test
  void testHexEncoding(@TempDir Path tempDir) throws IOException {

    final StreamArchiveConfig config =
      new StreamArchiveConfig(false, Arguments.of(Map.of(WriteableCsvArchive.OPTION_BINARY_ENCODING, "hex")));

    final String expectedCsv =
      """
        0,0,0,0001
        1,1,1,0203
        """;

    testTileOptions(tempDir, config, expectedCsv);
  }

  private void testTileOptions(Path tempDir, StreamArchiveConfig config, String expectedCsv) throws IOException {

    final Path csvFile = tempDir.resolve("mbtiles.json");

    try (var archive = WriteableCsvArchive.newWriteToFile(csvFile, config)) {
      archive.initialize(defaultMetadata);
      try (var tileWriter = archive.newTileWriter()) {
        tileWriter.write(new TileEncodingResult(TileCoord.ofXYZ(0, 0, 0), new byte[]{0, 1}, OptionalLong.empty()));
        tileWriter.write(new TileEncodingResult(TileCoord.ofXYZ(1, 1, 1), new byte[]{2, 3}, OptionalLong.empty()));
      }
      archive.finish(defaultMetadata);
    }

    assertEquals(expectedCsv, Files.readString(csvFile));

    assertEquals(Set.of(csvFile), Files.list(tempDir).collect(Collectors.toUnmodifiableSet()));
  }
}
