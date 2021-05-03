package com.onthegomap.flatmap.write;

import com.onthegomap.flatmap.geo.TileCoord;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;
import java.util.Set;

public class MbtilesTestUtils {

  static Set<Mbtiles.TileEntry> getAll(Mbtiles db) throws SQLException {
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
