package com.onthegomap.planetiler.archive;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import com.onthegomap.planetiler.util.TileSizeStats;
import java.util.List;
import org.junit.jupiter.api.Test;

class TileEncoderTest {

  @Test
  void encodedIsEqualToItself() {
    var encoded = new TileEncoder.Encoded(new byte[]{1, 2, 3}, statsForLayer("poi"));
    var sameInstance = encoded;

    assertEquals(encoded, sameInstance);
  }

  @Test
  void encodedWithEqualBytesAndStatsAreEqual() {
    var encoded = new TileEncoder.Encoded(new byte[]{1, 2, 3}, statsForLayer("poi"));
    var sameContents = new TileEncoder.Encoded(new byte[]{1, 2, 3}, statsForLayer("poi"));

    assertEquals(encoded, sameContents);
    assertEquals(encoded.hashCode(), sameContents.hashCode());
  }

  @Test
  void encodedWithDifferentBytesAreNotEqual() {
    var encoded = new TileEncoder.Encoded(new byte[]{1, 2, 3}, statsForLayer("poi"));
    var otherBytes = new TileEncoder.Encoded(new byte[]{1, 2, 4}, statsForLayer("poi"));

    assertNotEquals(encoded, otherBytes);
  }

  @Test
  void encodedWithDifferentLayerStatsAreNotEqual() {
    var encoded = new TileEncoder.Encoded(new byte[]{1, 2, 3}, statsForLayer("poi"));
    var otherStats = new TileEncoder.Encoded(new byte[]{1, 2, 3}, statsForLayer("road"));

    assertNotEquals(encoded, otherStats);
  }

  @Test
  void encodedIsNotEqualToOtherTypes() {
    var encoded = new TileEncoder.Encoded(new byte[]{1, 2, 3}, statsForLayer("poi"));

    assertNotEquals(encoded, new Object());
  }

  @Test
  void encodedToStringShowsTheByteCountInsteadOfTheBytes() {
    var stats = statsForLayer("poi");
    var encoded = new TileEncoder.Encoded(new byte[]{1, 2, 3}, stats);

    assertEquals("Encoded{bytes=byte[3], layerStats=" + stats + "}", encoded.toString());
  }

  /** Stats of a tile with one layer; only the layer name matters to these tests. */
  private static List<TileSizeStats.LayerStats> statsForLayer(String layer) {
    return List.of(new TileSizeStats.LayerStats(layer, 10, 1, 1, 4, 1, 1));
  }
}
