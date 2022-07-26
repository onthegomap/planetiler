package com.onthegomap.planetiler.geo;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;


class TileCoordTest {

  @ParameterizedTest
  @CsvSource({
    "0,0,0,0",
    "0,1,1,1",
    "0,0,1,2",
    "1,1,1,3",
    "1,0,1,4",
    "0,3,2,5",
    "0,2,2,6",
    "0,1,2,7",
    "0,0,2,8",
    "1,3,2,9",
    "1,2,2,10",
    "1,1,2,11",
    "1,0,2,12",
    "2,3,2,13",
    "2,2,2,14",
    "2,1,2,15",
    "2,0,2,16",
    "3,3,2,17",
    "3,2,2,18",
    "3,1,2,19",
    "3,0,2,20",
    "0,0,15,357946708",
    "0,32767,15,357913941",
    "32767,0,15,1431655764",
    "32767,32767,15,1431622997"
  })
  void testTileOrder(int x, int y, int z, int i) {
    int encoded = TileCoord.ofXYZ(x, y, z).encoded();
    assertEquals(i, encoded);
    TileCoord decoded = TileCoord.decode(i);
    assertEquals(decoded.x(), x, "x");
    assertEquals(decoded.y(), y, "y");
    assertEquals(decoded.z(), z, "z");
  }

  @Test
  void testTileSortOrderRespectZ() {
    int last = Integer.MIN_VALUE;
    for (int z = 0; z <= 15; z++) {
      int encoded = TileCoord.ofXYZ(0, 0, z).encoded();
      if (encoded < last) {
        fail("encoded value for z" + (z - 1) + " (" + last + ") is not less than z" + z + " (" + encoded + ")");
      }
      last = encoded;
    }
  }

  @ParameterizedTest
  @CsvSource({
    "0,0,0,0",
    "0,1,1,0",
    "1,1,1,0.5",
    "0,3,2,0"
  })
  void testTileProgressOnLevel(int x, int y, int z, double p) {
    double progress =
      TileCoord.ofXYZ(x, y, z).progressOnLevel(TileExtents.computeFromWorldBounds(15, GeoUtils.WORLD_BOUNDS));
    assertEquals(p, progress);
  }
}
