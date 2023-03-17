package com.onthegomap.planetiler.mbtiles;

import static com.onthegomap.planetiler.TestUtils.assertSameJson;
import static org.junit.jupiter.api.Assertions.*;

import com.google.common.math.IntMath;
import com.onthegomap.planetiler.TestUtils;
import com.onthegomap.planetiler.archive.TileArchiveMetadata;
import com.onthegomap.planetiler.archive.TileEncodingResult;
import com.onthegomap.planetiler.geo.TileCoord;
import com.onthegomap.planetiler.util.LayerStats;
import java.io.IOException;
import java.math.RoundingMode;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.locationtech.jts.geom.CoordinateXY;
import org.locationtech.jts.geom.Envelope;

class MbtilesTest {

  private static final int MAX_PARAMETERS_IN_PREPARED_STATEMENT = 999;
  private static final int TILES_BATCH = MAX_PARAMETERS_IN_PREPARED_STATEMENT / 4;
  private static final int TILES_SHALLOW_BATCH = MAX_PARAMETERS_IN_PREPARED_STATEMENT / 4;
  private static final int TILES_DATA_BATCH = MAX_PARAMETERS_IN_PREPARED_STATEMENT / 2;


  private static void testWriteTiles(int howMany, boolean skipIndexCreation, boolean optimize, boolean compactDb)
    throws IOException, SQLException {
    try (Mbtiles db = Mbtiles.newInMemoryDatabase(Map.of("compact", Boolean.toString(compactDb)))) {
      if (skipIndexCreation) {
        db.createTablesWithoutIndexes();
      } else {
        db.createTablesWithIndexes();
      }

      assertNull(db.getTile(0, 0, 0));
      Set<Mbtiles.TileEntry> expected = new TreeSet<>();
      try (var writer = db.newTileWriter()) {
        for (int i = 0; i < howMany; i++) {
          var dataHash = i - (i % 2);
          var dataBase = howMany + dataHash;
          var entry = new Mbtiles.TileEntry(TileCoord.ofXYZ(i, i + 1, 14), new byte[]{
            (byte) dataBase,
            (byte) (dataBase >> 8),
            (byte) (dataBase >> 16),
            (byte) (dataBase >> 24)
          });
          writer.write(new TileEncodingResult(entry.tile(), entry.bytes(), OptionalLong.of(dataHash)));
          expected.add(entry);
        }
      }

      if (optimize) {
        db.vacuumAnalyze();
      }
      var all = TestUtils.getAllTiles(db);
      assertEquals(howMany, all.size());
      assertEquals(expected, all);
      assertEquals(expected.stream().map(Mbtiles.TileEntry::tile).collect(Collectors.toSet()),
        db.getAllTileCoords().stream().collect(Collectors.toSet()));
      for (var expectedEntry : expected) {
        var tile = expectedEntry.tile();
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
    testWriteTiles(howMany, false, false, false);
  }

  @ParameterizedTest
  @ValueSource(ints = {0, 1, TILES_DATA_BATCH, TILES_DATA_BATCH + 1, 2 * TILES_DATA_BATCH, 2 * TILES_DATA_BATCH + 1,
    TILES_SHALLOW_BATCH, TILES_SHALLOW_BATCH + 1, 2 * TILES_SHALLOW_BATCH, 2 * TILES_SHALLOW_BATCH + 1})
  void testWriteTilesDifferentSizeInCompactMode(int howMany) throws IOException, SQLException {
    testWriteTiles(howMany, false, false, true);
  }

  @Test
  void testSkipIndexCreation() throws IOException, SQLException {
    testWriteTiles(10, true, false, false);
  }

  @Test
  void testVacuumAnalyze() throws IOException, SQLException {
    testWriteTiles(10, false, true, false);
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
    roundTripMetadata(new TileArchiveMetadata(
      "MyName",
      "MyDescription",
      "MyAttribution",
      "MyVersion",
      "baselayer",
      TileArchiveMetadata.MVT_FORMAT,
      new Envelope(1, 2, 3, 4),
      new CoordinateXY(5, 6),
      7d,
      8,
      9,
      List.of(new LayerStats.VectorLayer("MyLayer", Map.of())),
      Map.of("other key", "other value")
    ));
  }

  @Test
  void testRoundTripMinimalMetadata() throws IOException {
    var empty =
      new TileArchiveMetadata(null, null, null, null, null, null, null, null, null, null, null, null, Map.of());
    roundTripMetadata(empty);
    try (Mbtiles db = Mbtiles.newInMemoryDatabase()) {
      db.createTablesWithoutIndexes();
      assertEquals(empty, db.metadata());
    }
  }

  private static void roundTripMetadata(TileArchiveMetadata metadata) throws IOException {
    try (Mbtiles db = Mbtiles.newInMemoryDatabase()) {
      db.createTablesWithoutIndexes();
      var metadataTable = db.metadataTable();
      metadataTable.set(metadata);
      assertEquals(metadata, metadataTable.get());
    }
  }

  private void testMetadataJson(Mbtiles.MetadataJson object, String expected) throws IOException {
    try (Mbtiles db = Mbtiles.newInMemoryDatabase()) {
      var metadata = db.createTablesWithoutIndexes().metadataTable();
      metadata.setJson(object);
      var actual = metadata.getAll().get("json");
      assertSameJson(expected, actual);
    }
  }

  @Test
  void testMetadataJsonNoLayers() throws IOException {
    testMetadataJson(new Mbtiles.MetadataJson(), """
      {
        "vector_layers": []
      }
      """);
  }

  @Test
  void testFullMetadataJson() throws IOException {
    testMetadataJson(new Mbtiles.MetadataJson(
      new LayerStats.VectorLayer(
        "full",
        Map.of(
          "NUMBER_FIELD", LayerStats.FieldType.NUMBER,
          "STRING_FIELD", LayerStats.FieldType.STRING,
          "boolean field", LayerStats.FieldType.BOOLEAN
        )
      ).withDescription("full description")
        .withMinzoom(0)
        .withMaxzoom(5),
      new LayerStats.VectorLayer(
        "partial",
        Map.of()
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
