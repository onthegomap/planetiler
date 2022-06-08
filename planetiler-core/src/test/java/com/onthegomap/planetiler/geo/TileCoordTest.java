package com.onthegomap.planetiler.geo;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class TileCoordTest {

  @ParameterizedTest
  @CsvSource({
    "0,0,0",
    "0,0,1",
    "0,1,1",
    "1,1,1",
    "100,100,14",
    "0,0,14",
    "16383,0,14",
    "0,16383,14",
    "16363,16363,14",
    "0,0,15",
    "32767,0,15",
    "0,32767,15",
    "32767,32767,15"
  })
  void testTileCoord(int x, int y, int z) {
    TileCoord coord1 = TileCoord.ofXYZ(x, y, z);
    TileCoord coord2 = TileCoord.decode(coord1.encoded());
    assertEquals(coord1.x(), coord2.x(), "x");
    assertEquals(coord1.y(), coord2.y(), "y");
    assertEquals(coord1.z(), coord2.z(), "z");
    assertEquals(coord1, coord2);
  }

  @ParameterizedTest
  @CsvSource({
    "0,0,0,0",
    "0,0,1,1",
    "0,1,1,2",
    "1,1,1,3",
    "1,0,1,4",
    "0,0,2,5",
  })
  void testTileOrderHilbert(int x, int y, int z, int i) {
    int encoded = TileCoord.ofXYZ(x, y, z).encoded();
    assertEquals(i, encoded);
  }
}
