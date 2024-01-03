package com.onthegomap.planetiler.mbtiles;

import static com.onthegomap.planetiler.TestUtils.assertSameJson;
import static org.junit.jupiter.api.Assertions.*;

import com.google.common.math.IntMath;
import com.onthegomap.planetiler.TestUtils;
import com.onthegomap.planetiler.archive.Tile;
import com.onthegomap.planetiler.archive.TileArchiveMetadata;
import com.onthegomap.planetiler.archive.TileCompression;
import com.onthegomap.planetiler.archive.TileEncodingResult;
import com.onthegomap.planetiler.config.Arguments;
import com.onthegomap.planetiler.geo.TileCoord;
import com.onthegomap.planetiler.util.LayerAttrStats;
import java.io.IOException;
import java.math.RoundingMode;
import java.nio.file.Path;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Envelope;

class MbtilesTest {

  private static final int MAX_PARAMETERS_IN_PREPARED_STATEMENT = 999;
  private static final int TILES_BATCH = MAX_PARAMETERS_IN_PREPARED_STATEMENT / 4;
  private static final int TILES_SHALLOW_BATCH = MAX_PARAMETERS_IN_PREPARED_STATEMENT / 4;
  private static final int TILES_DATA_BATCH = MAX_PARAMETERS_IN_PREPARED_STATEMENT / 2;


  private static void testWriteTiles(Path path, int howMany, boolean skipIndexCreation, boolean optimize,
    boolean compactDb) throws IOException, SQLException {
    var options = Arguments.of("compact", Boolean.toString(compactDb));
    try (
      Mbtiles db = path == null ? Mbtiles.newInMemoryDatabase(options) : Mbtiles.newWriteToFileDatabase(path, options)
    ) {
      if (skipIndexCreation) {
        db.createTablesWithoutIndexes();
      } else {
        db.createTablesWithIndexes();
      }

      assertNull(db.getTile(0, 0, 0));
      Set<Tile> expected = new TreeSet<>();
      try (var writer = db.newTileWriter()) {
        for (int i = 0; i < howMany; i++) {
          var dataHash = i - (i % 2);
          var dataBase = howMany + dataHash;
          var entry = new Tile(TileCoord.ofXYZ(i, i + 1, 14), new byte[]{
            (byte) dataBase,
            (byte) (dataBase >> 8),
            (byte) (dataBase >> 16),
            (byte) (dataBase >> 24)
          });
          writer.write(new TileEncodingResult(entry.coord(), entry.bytes(), OptionalLong.of(dataHash)));
          expected.add(entry);
        }
      }

      if (optimize) {
        db.vacuumAnalyze();
      }
      var all = TestUtils.getTiles(db);
      assertEquals(howMany, all.size());
      assertEquals(expected, all);
      assertEquals(expected.stream().map(Tile::coord).collect(Collectors.toSet()),
        db.getAllTileCoords().stream().collect(Collectors.toSet()));
      assertEquals(expected, db.getAllTiles().stream().collect(Collectors.toSet()));
      for (var expectedEntry : expected) {
        var tile = expectedEntry.coord();
        byte[] data = db.getTile(tile.x(), tile.y(), tile.z());
        assertArrayEquals(expectedEntry.bytes(), data);
      }
      assertEquals(compactDb, TestUtils.isCompactDb(db));
      if (compactDb) {
        assertEquals(IntMath.divide(howMany, 2, RoundingMode.CEILING), TestUtils.getTilesDataCount(db));
      }
    }
  }

  @ParameterizedTest
  @ValueSource(ints = {0, 1, TILES_BATCH, TILES_BATCH + 1, 2 * TILES_BATCH, 2 * TILES_BATCH + 1})
  void testWriteTilesDifferentSizeInNonCompactMode(int howMany) throws IOException, SQLException {
    testWriteTiles(null, howMany, false, false, false);
  }

  @ParameterizedTest
  @ValueSource(ints = {0, 1, TILES_DATA_BATCH, TILES_DATA_BATCH + 1, 2 * TILES_DATA_BATCH, 2 * TILES_DATA_BATCH + 1,
    TILES_SHALLOW_BATCH, TILES_SHALLOW_BATCH + 1, 2 * TILES_SHALLOW_BATCH, 2 * TILES_SHALLOW_BATCH + 1})
  void testWriteTilesDifferentSizeInCompactMode(int howMany) throws IOException, SQLException {
    testWriteTiles(null, howMany, false, false, true);
  }

  @Test
  void testSkipIndexCreation() throws IOException, SQLException {
    testWriteTiles(null, 10, true, false, false);
  }

  @Test
  void testVacuumAnalyze() throws IOException, SQLException {
    testWriteTiles(null, 10, false, true, false);
  }

  @Test
  void testWriteToFile(@TempDir Path tmpDir) throws IOException, SQLException {
    testWriteTiles(tmpDir.resolve("archive.mbtiles"), 10, false, false, true);
  }

  @Test
  void testCustomPragma() throws IOException, SQLException {
    try (
      Mbtiles db = Mbtiles.newInMemoryDatabase(Arguments.of(
        "cache-size", "123",
        "garbage", "456"
      ));
    ) {
      int result = db.connection().createStatement().executeQuery("pragma cache_size").getInt(1);
      assertEquals(123, result);
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testManualIndexCreationStatements(boolean compactDb) throws IOException, SQLException {
    try (Mbtiles db = Mbtiles.newInMemoryDatabase(compactDb)) {
      db.createTablesWithoutIndexes();

      List<String> indexCreationStmts = db.getManualIndexCreationStatements();
      assertFalse(indexCreationStmts.isEmpty());
      for (String indexCreationStmt : indexCreationStmts) {
        try (Statement stmt = db.connection().createStatement()) {
          assertDoesNotThrow(() -> stmt.execute(indexCreationStmt));
        }
      }
    }
  }

  @Test
  void testRoundTripMetadata() throws IOException {
    roundTripMetadata(metadataWithJson(
      TileArchiveMetadata.TileArchiveMetadataJson.create(
        List.of(new LayerAttrStats.VectorLayer("MyLayer", Map.of()))
      )
    ));
  }

  @Test
  void testMetadataWithoutCompressionAssumesGzip() throws IOException {

    final TileArchiveMetadata metadataIn = new TileArchiveMetadata(
      "MyName",
      "MyDescription",
      "MyAttribution",
      "MyVersion",
      "baselayer",
      TileArchiveMetadata.MVT_FORMAT,
      new Envelope(1, 2, 3, 4),
      new Coordinate(5, 6, 7d),
      8,
      9,
      TileArchiveMetadata.TileArchiveMetadataJson.create(
        List.of(new LayerAttrStats.VectorLayer("MyLayer", Map.of()))

      ),
      Map.of("other key", "other value"),
      null
    );

    final TileArchiveMetadata expectedMetadataOut = new TileArchiveMetadata(
      "MyName",
      "MyDescription",
      "MyAttribution",
      "MyVersion",
      "baselayer",
      TileArchiveMetadata.MVT_FORMAT,
      new Envelope(1, 2, 3, 4),
      new Coordinate(5, 6, 7d),
      8,
      9,
      TileArchiveMetadata.TileArchiveMetadataJson.create(
        List.of(new LayerAttrStats.VectorLayer("MyLayer", Map.of()))
      ),
      Map.of("other key", "other value"),
      TileCompression.GZIP
    );

    roundTripMetadata(metadataIn, expectedMetadataOut);
  }

  @Test
  void testRoundTripMinimalMetadata() throws IOException {
    var empty =
      new TileArchiveMetadata(null, null, null, null, null, null, null, null, null, null, null, Map.of(),
        TileCompression.GZIP);
    roundTripMetadata(empty);
    try (Mbtiles db = Mbtiles.newInMemoryDatabase()) {
      db.createTablesWithoutIndexes();
      assertEquals(empty, db.metadata());
    }
  }

  private static void roundTripMetadata(TileArchiveMetadata metadata) throws IOException {
    roundTripMetadata(metadata, metadata);
  }

  private static void roundTripMetadata(TileArchiveMetadata metadata, TileArchiveMetadata expectedOut)
    throws IOException {
    try (Mbtiles db = Mbtiles.newInMemoryDatabase()) {
      db.createTablesWithoutIndexes();
      var metadataTable = db.metadataTable();
      metadataTable.set(metadata);
      assertEquals(expectedOut, metadataTable.get());
    }
  }

  private static TileArchiveMetadata metadataWithJson(TileArchiveMetadata.TileArchiveMetadataJson metadataJson) {
    return new TileArchiveMetadata(
      "MyName",
      "MyDescription",
      "MyAttribution",
      "MyVersion",
      "baselayer",
      TileArchiveMetadata.MVT_FORMAT,
      new Envelope(1, 2, 3, 4),
      new Coordinate(5, 6, 7d),
      8,
      9,
      metadataJson,
      Map.of("other key", "other value"),
      TileCompression.GZIP
    );
  }


  private void testMetadataJson(TileArchiveMetadata.TileArchiveMetadataJson metadataJson, String expected)
    throws IOException {
    try (Mbtiles db = Mbtiles.newInMemoryDatabase()) {
      var metadata = db.createTablesWithoutIndexes().metadataTable();
      metadata.set(metadataWithJson(metadataJson));
      var actual = metadata.getAll().get("json");
      assertSameJson(expected, actual);
    }
  }

  @Test
  void testMetadataJsonNoLayers() throws IOException {
    testMetadataJson(TileArchiveMetadata.TileArchiveMetadataJson.create(List.of()), """
      {
        "vector_layers": []
      }
      """);
  }

  @Test
  void testFullMetadataJson() throws IOException {
    testMetadataJson(new TileArchiveMetadata.TileArchiveMetadataJson(
      List.of(
        new LayerAttrStats.VectorLayer(
          "full",
          Map.of(
            "NUMBER_FIELD", LayerAttrStats.FieldType.NUMBER,
            "STRING_FIELD", LayerAttrStats.FieldType.STRING,
            "boolean field", LayerAttrStats.FieldType.BOOLEAN
          )
        ).withDescription("full description")
          .withMinzoom(0)
          .withMaxzoom(5),
        new LayerAttrStats.VectorLayer(
          "partial",
          Map.of()
        )
      )
    ), """
      {
        "vector_layers": [
          {
            "id": "full",
            "description": "full description",
            "minzoom": 0,
            "maxzoom": 5,
            "fields": {
              "NUMBER_FIELD": "Number",
              "STRING_FIELD": "String",
              "boolean field": "Boolean"
            }
          },
          {
            "id": "partial",
            "fields": {}
          }
        ]
      }
      """);
  }
}
