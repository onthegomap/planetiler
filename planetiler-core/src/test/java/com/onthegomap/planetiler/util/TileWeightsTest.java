package com.onthegomap.planetiler.util;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.onthegomap.planetiler.geo.TileCoord;
import org.junit.jupiter.api.Test;

class TileWeightsTest {
  @Test
  void test() {
    var weights = new TileWeights();
    assertEquals(0, weights.getWeight(TileCoord.ofXYZ(0, 0, 0)));
    assertEquals(0, weights.getZoomWeight(0));
    assertEquals(0, weights.getWeight(TileCoord.ofXYZ(0, 0, 1)));
    assertEquals(0, weights.getWeight(TileCoord.ofXYZ(1, 0, 1)));
    assertEquals(0, weights.getZoomWeight(1));

    weights.put(TileCoord.ofXYZ(0, 0, 0), 1);
    weights.put(TileCoord.ofXYZ(0, 0, 0), 2);
    weights.put(TileCoord.ofXYZ(0, 0, 1), 3);
    weights.put(TileCoord.ofXYZ(1, 0, 1), 4);

    assertEquals(3, weights.getWeight(TileCoord.ofXYZ(0, 0, 0)));
    assertEquals(3, weights.getZoomWeight(0));
    assertEquals(3, weights.getWeight(TileCoord.ofXYZ(0, 0, 1)));
    assertEquals(4, weights.getWeight(TileCoord.ofXYZ(1, 0, 1)));
    assertEquals(7, weights.getZoomWeight(1));
  }
}
