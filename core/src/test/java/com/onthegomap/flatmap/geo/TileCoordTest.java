package com.onthegomap.flatmap.geo;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

public class TileCoordTest {

  @ParameterizedTest
  @CsvSource({
    "0,0,0,0",
    "0,0,1,268435456",
    "0,1,1,268435457",
    "1,1,1,268451841",
    "100,100,14,-535232412",
  })
  public void testTileCoord(int x, int y, int z, int encoded) {
    TileCoord coord1 = TileCoord.ofXYZ(x, y, z);
    assertEquals(encoded, coord1.encoded());
    TileCoord coord2 = TileCoord.decode(encoded);
    assertEquals(coord1.x(), coord2.x());
    assertEquals(coord1.y(), coord2.y());
    assertEquals(coord1.z(), coord2.z());
    assertEquals(coord1, coord2);
  }

  @Test
  public void testThrowsPastZ14() {
    assertThrows(AssertionError.class, () -> TileCoord.ofXYZ(0, 0, 15));
  }
}
