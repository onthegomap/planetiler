package com.onthegomap.flatmap.write;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.onthegomap.flatmap.geo.TileCoord;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class MbtilesTest {

  private static final int BATCH = 999 / 4;

  public void testWriteTiles(int howMany, boolean deferIndexCreation, boolean optimize)
    throws IOException, SQLException {
    try (Mbtiles db = Mbtiles.newInMemoryDatabase()) {
      db
        .setupSchema()
        .tuneForWrites();
      if (!deferIndexCreation) {
        db.addIndex();
      }
      Set<Mbtiles.TileEntry> expected = new HashSet<>();
      try (var writer = db.newBatchedTileWriter()) {
        for (int i = 0; i < howMany; i++) {
          var entry = new Mbtiles.TileEntry(TileCoord.ofXYZ(i, i, 14), new byte[]{
            (byte) howMany,
            (byte) (howMany >> 8),
            (byte) (howMany >> 16),
            (byte) (howMany >> 24)
          });
          writer.write(entry.tile(), entry.bytes());
          expected.add(entry);
        }
      }
      if (deferIndexCreation) {
        db.addIndex();
      }
      if (optimize) {
        db.vacuumAnalyze();
      }
      var all = getAll(db);
      assertEquals(howMany, all.size());
      assertEquals(expected, all);
    }
  }

  @ParameterizedTest
  @ValueSource(ints = {0, 1, BATCH, BATCH + 1, 2 * BATCH, 2 * BATCH + 1})
  public void testWriteTilesDifferentSize(int howMany) throws IOException, SQLException {
    testWriteTiles(howMany, false, false);
  }

  @Test
  public void testDeferIndexCreation() throws IOException, SQLException {
    testWriteTiles(10, true, false);
  }

  @Test
  public void testVacuumAnalyze() throws IOException, SQLException {
    testWriteTiles(10, false, true);
  }

  private static Set<Mbtiles.TileEntry> getAll(Mbtiles db) throws SQLException {
    Set<Mbtiles.TileEntry> result = new HashSet<>();
    try (Statement statement = db.connection().createStatement()) {
      ResultSet rs = statement.executeQuery("select zoom_level, tile_column, tile_row, tile_data from tiles");
      while (rs.next()) {
        result.add(new Mbtiles.TileEntry(
          TileCoord.ofXYZ(
            rs.getInt("tile_column"),
            rs.getInt("tile_row"),
            rs.getInt("zoom_level")
          ),
          rs.getBytes("tile_data")
        ));
      }
    }
    return result;
  }
}
