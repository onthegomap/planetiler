package com.onthegomap.planetiler.mbtiles;

import static com.onthegomap.planetiler.TestUtils.assertSameJson;
import static org.junit.jupiter.api.Assertions.*;

import com.google.common.math.IntMath;
import com.onthegomap.planetiler.TestUtils;
import com.onthegomap.planetiler.geo.GeoUtils;
import com.onthegomap.planetiler.geo.TileCoord;
import java.io.IOException;
import java.math.RoundingMode;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.OptionalInt;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.locationtech.jts.geom.Envelope;

class MbtilesTest {

  private static final int MAX_PARAMETERS_IN_PREPARED_STATEMENT = 999;
  private static final int TILES_BATCH = MAX_PARAMETERS_IN_PREPARED_STATEMENT / 4;
  private static final int TILES_SHALLOW_BATCH = MAX_PARAMETERS_IN_PREPARED_STATEMENT / 4;
  private static final int TILES_DATA_BATCH = MAX_PARAMETERS_IN_PREPARED_STATEMENT / 2;


  private static final

    void testWriteTiles(int howMany, boolean deferIndexCreation, boolean optimize, boolean compactDb)
      throws IOException, SQLException {
    try (Mbtiles db = Mbtiles.newInMemoryDatabase(compactDb)) {
      db.createTables(deferIndexCreation);

      assertNull(db.getTile(0, 0, 0));
      Set<Mbtiles.TileEntry> expected = new TreeSet<>();
      try (var writer = db.newBatchedTileWriter()) {
        for (int i = 0; i < howMany; i++) {
          var dataHash = i - (i % 2);
          var dataBase = howMany + dataHash;
          var entry = new Mbtiles.TileEntry(TileCoord.ofXYZ(i, i + 1, 14), new byte[]{
            (byte) dataBase,
            (byte) (dataBase >> 8),
            (byte) (dataBase >> 16),
            (byte) (dataBase >> 24)
          });
          writer.write(new TileEncodingResult(entry.tile(), entry.bytes(), OptionalInt.of(dataHash)));
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
        new HashSet<>(db.getAllTileCoords()));
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
  void testDeferIndexCreation() throws IOException, SQLException {
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
      db.createTables(true);

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
  void testAddMetadata() throws IOException {
    Map<String, String> expected = new TreeMap<>();
    try (Mbtiles db = Mbtiles.newInMemoryDatabase()) {
      var metadata = db.createTables(true).metadata();
      metadata.setName("name value");
      expected.put("name", "name value");

      metadata.setFormat("pbf");
      expected.put("format", "pbf");

      metadata.setAttribution("attribution value");
      expected.put("attribution", "attribution value");

      metadata.setBoundsAndCenter(GeoUtils.toLatLonBoundsBounds(new Envelope(0.25, 0.75, 0.25, 0.75)));
      expected.put("bounds", "-90,-66.51326,90,66.51326");
      expected.put("center", "0,0,1");

      metadata.setDescription("description value");
      expected.put("description", "description value");

      metadata.setMinzoom(1);
      expected.put("minzoom", "1");

      metadata.setMaxzoom(13);
      expected.put("maxzoom", "13");

      metadata.setVersion("1.2.3");
      expected.put("version", "1.2.3");

      metadata.setTypeIsBaselayer();
      expected.put("type", "baselayer");

      assertEquals(expected, metadata.getAll());
    }
  }

  @Test
  void testAddMetadataWorldBounds() throws IOException {
    Map<String, String> expected = new TreeMap<>();
    try (Mbtiles db = Mbtiles.newInMemoryDatabase()) {
      var metadata = db.createTables(true).metadata();
      metadata.setBoundsAndCenter(GeoUtils.WORLD_LAT_LON_BOUNDS);
      expected.put("bounds", "-180,-85.05113,180,85.05113");
      expected.put("center", "0,0,0");

      assertEquals(expected, metadata.getAll());
    }
  }

  @Test
  void testAddMetadataSmallBounds() throws IOException {
    Map<String, String> expected = new TreeMap<>();
    try (Mbtiles db = Mbtiles.newInMemoryDatabase()) {
      var metadata = db.createTables(true).metadata();
      metadata.setBoundsAndCenter(new Envelope(-73.6632, -69.7598, 41.1274, 43.0185));
      expected.put("bounds", "-73.6632,41.1274,-69.7598,43.0185");
      expected.put("center", "-71.7115,42.07295,7");

      assertEquals(expected, metadata.getAll());
    }
  }

  private void testMetadataJson(Mbtiles.MetadataJson object, String expected) throws IOException {
    try (Mbtiles db = Mbtiles.newInMemoryDatabase()) {
      var metadata = db.createTables(true).metadata();
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
      new Mbtiles.MetadataJson.VectorLayer(
        "full",
        Map.of(
          "NUMBER_FIELD", Mbtiles.MetadataJson.FieldType.NUMBER,
          "STRING_FIELD", Mbtiles.MetadataJson.FieldType.STRING,
          "boolean field", Mbtiles.MetadataJson.FieldType.BOOLEAN
        )
      ).withDescription("full description")
        .withMinzoom(0)
        .withMaxzoom(5),
      new Mbtiles.MetadataJson.VectorLayer(
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
