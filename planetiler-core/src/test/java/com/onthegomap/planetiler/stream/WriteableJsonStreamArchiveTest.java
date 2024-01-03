package com.onthegomap.planetiler.stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.onthegomap.planetiler.TestUtils;
import com.onthegomap.planetiler.archive.TileEncodingResult;
import com.onthegomap.planetiler.config.Arguments;
import com.onthegomap.planetiler.geo.TileCoord;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class WriteableJsonStreamArchiveTest {

  private static final StreamArchiveConfig defaultConfig = new StreamArchiveConfig(false, Arguments.of());

  @Test
  void testWriteToSingleFile(@TempDir Path tempDir) throws IOException {

    final Path csvFile = tempDir.resolve("out.json");

    try (var archive = WriteableJsonStreamArchive.newWriteToFile(csvFile, defaultConfig)) {
      archive.initialize();
      try (var tileWriter = archive.newTileWriter()) {
        tileWriter.write(new TileEncodingResult(TileCoord.ofXYZ(0, 0, 0), new byte[]{0}, OptionalLong.empty()));
        tileWriter.write(new TileEncodingResult(TileCoord.ofXYZ(1, 2, 3), new byte[]{1}, OptionalLong.of(1)));
      }
      archive.finish(TestUtils.MIN_METADATA_DESERIALIZED);
    }

    assertEqualsDelimitedJson(
      """
        {"type":"initialization"}
        {"type":"tile","x":0,"y":0,"z":0,"encodedData":"AA=="}
        {"type":"tile","x":1,"y":2,"z":3,"encodedData":"AQ=="}
        {"type":"finish","metadata":%s}
        """.formatted(TestUtils.MIN_METADATA_SERIALIZED),
      Files.readString(csvFile)
    );

    assertEquals(Set.of(csvFile), Files.list(tempDir).collect(Collectors.toUnmodifiableSet()));
  }

  @Test
  void testWriteToMultipleFiles(@TempDir Path tempDir) throws IOException {

    final Path csvFilePrimary = tempDir.resolve("out.json");
    final Path csvFileSecondary = tempDir.resolve("out.json1");
    final Path csvFileTertiary = tempDir.resolve("out.json2");

    final var tile0 = new TileEncodingResult(TileCoord.ofXYZ(11, 12, 1), new byte[]{0}, OptionalLong.empty());
    final var tile1 = new TileEncodingResult(TileCoord.ofXYZ(21, 22, 2), new byte[]{1}, OptionalLong.empty());
    final var tile2 = new TileEncodingResult(TileCoord.ofXYZ(31, 32, 3), new byte[]{2}, OptionalLong.empty());
    final var tile3 = new TileEncodingResult(TileCoord.ofXYZ(41, 42, 4), new byte[]{3}, OptionalLong.empty());
    final var tile4 = new TileEncodingResult(TileCoord.ofXYZ(51, 52, 5), new byte[]{4}, OptionalLong.empty());
    try (var archive = WriteableJsonStreamArchive.newWriteToFile(csvFilePrimary, defaultConfig)) {
      archive.initialize();
      try (var tileWriter = archive.newTileWriter()) {
        tileWriter.write(tile0);
        tileWriter.write(tile1);
      }
      try (var tileWriter = archive.newTileWriter()) {
        tileWriter.write(tile2);
        tileWriter.write(tile3);
      }
      try (var tileWriter = archive.newTileWriter()) {
        tileWriter.write(tile4);
      }
      archive.finish(TestUtils.MAX_METADATA_DESERIALIZED);
    }

    assertEqualsDelimitedJson(
      """
        {"type":"initialization"}
        {"type":"tile","x":11,"y":12,"z":1,"encodedData":"AA=="}
        {"type":"tile","x":21,"y":22,"z":2,"encodedData":"AQ=="}
        {"type":"finish","metadata":%s}
        """.formatted(TestUtils.MAX_METADATA_SERIALIZED),
      Files.readString(csvFilePrimary)
    );

    assertEqualsDelimitedJson(
      """
        {"type":"tile","x":31,"y":32,"z":3,"encodedData":"Ag=="}
        {"type":"tile","x":41,"y":42,"z":4,"encodedData":"Aw=="}
        """,
      Files.readString(csvFileSecondary)
    );

    assertEqualsDelimitedJson(
      """
        {"type":"tile","x":51,"y":52,"z":5,"encodedData":"BA=="}
        """,
      Files.readString(csvFileTertiary)
    );

    assertEquals(
      Set.of(csvFilePrimary, csvFileSecondary, csvFileTertiary),
      Files.list(tempDir).collect(Collectors.toUnmodifiableSet())
    );
  }

  @Test
  void testTilesOnly(@TempDir Path tempDir) throws IOException {

    final StreamArchiveConfig config = new StreamArchiveConfig(false, Arguments.of(Map.of("tiles_only", "true")));

    final String expectedCsv = """
      {"type":"tile","x":0,"y":0,"z":0,"encodedData":"AA=="}
      {"type":"tile","x":1,"y":2,"z":3,"encodedData":"AQ=="}
      """;

    testTileOptions(tempDir, config, expectedCsv);
  }

  @Test
  void testRootValueSeparator(@TempDir Path tempDir) throws IOException {

    final StreamArchiveConfig config =
      new StreamArchiveConfig(false, Arguments.of(Map.of("root_value_separator", "' '")));

    final String expectedJson =
      """
        {"type":"initialization"}
        {"type":"tile","x":0,"y":0,"z":0,"encodedData":"AA=="}
        {"type":"tile","x":1,"y":2,"z":3,"encodedData":"AQ=="}
        {"type":"finish","metadata":%s}
        """.formatted(TestUtils.MAX_METADATA_SERIALIZED)
        .replace('\n', ' ');

    testTileOptions(tempDir, config, expectedJson);

    assertFalse(Files.readString(tempDir.resolve("out.json")).contains("\n"));
  }

  private void testTileOptions(Path tempDir, StreamArchiveConfig config, String expectedJson) throws IOException {

    final Path csvFile = tempDir.resolve("out.json");

    try (var archive = WriteableJsonStreamArchive.newWriteToFile(csvFile, config)) {
      archive.initialize();
      try (var tileWriter = archive.newTileWriter()) {
        tileWriter.write(new TileEncodingResult(TileCoord.ofXYZ(0, 0, 0), new byte[]{0}, OptionalLong.empty()));
        tileWriter.write(new TileEncodingResult(TileCoord.ofXYZ(1, 2, 3), new byte[]{1}, OptionalLong.empty()));
      }
      archive.finish(TestUtils.MAX_METADATA_DESERIALIZED);
    }

    assertEqualsDelimitedJson(expectedJson, Files.readString(csvFile));

    assertEquals(Set.of(csvFile), Files.list(tempDir).collect(Collectors.toUnmodifiableSet()));
  }

  private static void assertEqualsDelimitedJson(String expectedJson, String actualJson) {
    assertEquals(readDelimitedNodes(expectedJson), readDelimitedNodes(actualJson));
  }

  private static List<JsonNode> readDelimitedNodes(String json) {
    try {
      return ImmutableList.copyOf(new ObjectMapper().readerFor(JsonNode.class).readValues(json));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

}
