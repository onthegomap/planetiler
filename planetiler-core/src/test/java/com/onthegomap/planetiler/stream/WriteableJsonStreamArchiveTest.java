package com.onthegomap.planetiler.stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.onthegomap.planetiler.archive.TileArchiveMetadata;
import com.onthegomap.planetiler.archive.TileCompression;
import com.onthegomap.planetiler.archive.TileEncodingResult;
import com.onthegomap.planetiler.config.Arguments;
import com.onthegomap.planetiler.geo.TileCoord;
import com.onthegomap.planetiler.util.LayerAttrStats;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.locationtech.jts.geom.CoordinateXY;
import org.locationtech.jts.geom.Envelope;

class WriteableJsonStreamArchiveTest {

  private static final StreamArchiveConfig defaultConfig = new StreamArchiveConfig(false, Arguments.of());
  private static final TileArchiveMetadata maxMetadataIn =
    new TileArchiveMetadata("name", "description", "attribution", "version", "type", "format", new Envelope(0, 1, 2, 3),
      new CoordinateXY(1.3, 3.7), 1.0, 2, 3,
      List.of(
        new LayerAttrStats.VectorLayer("vl0",
          ImmutableMap.of("1", LayerAttrStats.FieldType.BOOLEAN, "2", LayerAttrStats.FieldType.NUMBER, "3",
            LayerAttrStats.FieldType.STRING),
          Optional.of("description"), OptionalInt.of(1), OptionalInt.of(2)),
        new LayerAttrStats.VectorLayer("vl1",
          Map.of(),
          Optional.empty(), OptionalInt.empty(), OptionalInt.empty())
      ),
      ImmutableMap.of("a", "b", "c", "d"),
      TileCompression.GZIP);
  private static final String maxMetadataOut = """
    {
      "name":"name",
      "description":"description",
      "attribution":"attribution",
      "version":"version",
      "type":"type",
      "format":"format",
      "zoom":1.0,
      "minzoom":2,
      "maxzoom":3,
      "compression":"gzip",
      "bounds":{
        "minX":0.0,
        "maxX":1.0,
        "minY":2.0,
        "maxY":3.0
      },
      "center":{
        "x":1.3,"y":3.7
      },
      "vectorLayers":[
        {
          "id":"vl0",
          "fields":{
            "1":"Boolean",
            "2":"Number",
            "3":"String"
          },
          "description":"description",
          "minzoom":1,
          "maxzoom":2
        },
        {
          "id":"vl1",
          "fields":{}
        }
      ],
      "a":"b",
      "c":"d"
    }""".lines().map(String::trim).collect(Collectors.joining(""));

  private static final TileArchiveMetadata minMetadataIn =
    new TileArchiveMetadata(null, null, null, null, null, null, null, null, null, null, null, null, null, null);
  private static final String MIN_METADATA_OUT = "{}";

  @Test
  void testWriteToSingleFile(@TempDir Path tempDir) throws IOException {

    final Path csvFile = tempDir.resolve("out.json");

    try (var archive = WriteableJsonStreamArchive.newWriteToFile(csvFile, defaultConfig)) {
      archive.initialize(maxMetadataIn);
      try (var tileWriter = archive.newTileWriter()) {
        tileWriter.write(new TileEncodingResult(TileCoord.ofXYZ(0, 0, 0), new byte[]{0}, OptionalLong.empty()));
        tileWriter.write(new TileEncodingResult(TileCoord.ofXYZ(1, 2, 3), new byte[]{1}, OptionalLong.of(1)));
      }
      archive.finish(minMetadataIn);
    }

    assertEqualsDelimitedJson(
      """
        {"type":"initialization","metadata":%s}
        {"type":"tile","x":0,"y":0,"z":0,"encodedData":"AA=="}
        {"type":"tile","x":1,"y":2,"z":3,"encodedData":"AQ=="}
        {"type":"finish","metadata":%s}
        """.formatted(maxMetadataOut, MIN_METADATA_OUT),
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
      archive.initialize(minMetadataIn);
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
      archive.finish(maxMetadataIn);
    }

    assertEqualsDelimitedJson(
      """
        {"type":"initialization","metadata":%s}
        {"type":"tile","x":11,"y":12,"z":1,"encodedData":"AA=="}
        {"type":"tile","x":21,"y":22,"z":2,"encodedData":"AQ=="}
        {"type":"finish","metadata":%s}
        """.formatted(MIN_METADATA_OUT, maxMetadataOut),
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
        {"type":"initialization","metadata":%s}
        {"type":"tile","x":0,"y":0,"z":0,"encodedData":"AA=="}
        {"type":"tile","x":1,"y":2,"z":3,"encodedData":"AQ=="}
        {"type":"finish","metadata":%s}
        """.formatted(MIN_METADATA_OUT, maxMetadataOut)
        .replace('\n', ' ');

    testTileOptions(tempDir, config, expectedJson);

    assertFalse(Files.readString(tempDir.resolve("out.json")).contains("\n"));
  }

  private void testTileOptions(Path tempDir, StreamArchiveConfig config, String expectedJson) throws IOException {

    final Path csvFile = tempDir.resolve("out.json");

    try (var archive = WriteableJsonStreamArchive.newWriteToFile(csvFile, config)) {
      archive.initialize(minMetadataIn);
      try (var tileWriter = archive.newTileWriter()) {
        tileWriter.write(new TileEncodingResult(TileCoord.ofXYZ(0, 0, 0), new byte[]{0}, OptionalLong.empty()));
        tileWriter.write(new TileEncodingResult(TileCoord.ofXYZ(1, 2, 3), new byte[]{1}, OptionalLong.empty()));
      }
      archive.finish(maxMetadataIn);
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
