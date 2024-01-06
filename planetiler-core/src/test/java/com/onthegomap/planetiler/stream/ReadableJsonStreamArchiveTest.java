package com.onthegomap.planetiler.stream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import com.onthegomap.planetiler.TestUtils;
import com.onthegomap.planetiler.archive.Tile;
import com.onthegomap.planetiler.config.Arguments;
import com.onthegomap.planetiler.geo.TileCoord;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class ReadableJsonStreamArchiveTest {

  @Test
  void testSimple(@TempDir Path tempDir) throws IOException {

    final Path jsonFile = tempDir.resolve("in.json");
    final String json = """
      {"type":"initialization"}
      {"type":"tile","x":0,"y":0,"z":0,"encodedData":"AA=="}
      {"type":"tile","x":1,"y":2,"z":3,"encodedData":"AQ=="}
      {"type":"finish","metadata":%s}
      """.formatted(TestUtils.MAX_METADATA_SERIALIZED);

    Files.writeString(jsonFile, json);
    final StreamArchiveConfig config = new StreamArchiveConfig(false, Arguments.of());

    final List<Tile> expectedTiles = List.of(
      new Tile(TileCoord.ofXYZ(0, 0, 0), new byte[]{0}),
      new Tile(TileCoord.ofXYZ(1, 2, 3), new byte[]{1})
    );
    try (var reader = ReadableJsonStreamArchive.newReader(jsonFile, config)) {
      try (var s = reader.getAllTiles().stream()) {
        assertEquals(expectedTiles, s.toList());
      }
      try (var s = reader.getAllTiles().stream()) {
        assertEquals(expectedTiles, s.toList());
      }
      assertEquals(TestUtils.MAX_METADATA_DESERIALIZED, reader.metadata());
      assertEquals(TestUtils.MAX_METADATA_DESERIALIZED, reader.metadata());
      assertArrayEquals(expectedTiles.get(1).bytes(), reader.getTile(TileCoord.ofXYZ(1, 2, 3)));
      assertArrayEquals(expectedTiles.get(0).bytes(), reader.getTile(0, 0, 0));
      assertNull(reader.getTile(4, 5, 6));
    }
  }
}
