package com.onthegomap.planetiler.geo;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

public class TileCoordTest {

  @ParameterizedTest
  @CsvSource({
    "0,0,0",
    "0,0,1",
    "0,1,1",
    "1,1,1",
    "100,100,14",
  })
  public void testTileCoord(int x, int y, int z) {
    TileCoord coord1 = TileCoord.ofXYZ(x, y, z);
    TileCoord coord2 = TileCoord.decode(coord1.encoded());
    assertEquals(coord1.x(), coord2.x(), "x");
    assertEquals(coord1.y(), coord2.y(), "y");
    assertEquals(coord1.z(), coord2.z(), "z");
    assertEquals(coord1, coord2);
  }

  @Test
  public void testTileSortOrderRespectZ() {
    int last = Integer.MIN_VALUE;
    for (int z = 0; z <= 14; z++) {
      int encoded = TileCoord.ofXYZ(0, 0, z).encoded();
      if (encoded < last) {
        fail("encoded value for z" + (z - 1) + " (" + last + ") is not less than z" + z + " (" + encoded + ")");
      }
      last = encoded;
    }
  }

  @Test
  public void testTileSortOrderFlipY() {
    for (int z = 1; z <= 14; z++) {
      int encoded1 = TileCoord.ofXYZ(0, 1, z).encoded();
      int encoded2 = TileCoord.ofXYZ(0, 0, z).encoded();
      if (encoded2 < encoded1) {
        fail("encoded value for y=1 is not less than y=0 at z=" + z);
      }
    }
  }

  @Test
  public void testThrowsPastZ14() {
    assertThrows(AssertionError.class, () -> TileCoord.ofXYZ(0, 0, 15));
  }
}
