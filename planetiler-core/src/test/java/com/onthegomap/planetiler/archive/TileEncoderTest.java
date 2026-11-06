package com.onthegomap.planetiler.archive;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import com.onthegomap.planetiler.util.TileSizeStats;
import java.util.List;
import org.junit.jupiter.api.Test;

class TileEncoderTest {

  private static final List<TileSizeStats.LayerStats> POI_STATS =
    List.of(new TileSizeStats.LayerStats("poi", 10, 1, 1, 4, 1, 1));
  private static final List<TileSizeStats.LayerStats> ROAD_STATS =
    List.of(new TileSizeStats.LayerStats("road", 10, 1, 1, 4, 1, 1));

  @Test
  void encodedIsEqualToItself() {
    var encoded = new TileEncoder.Encoded(new byte[]{1, 2, 3}, POI_STATS);
    var sameInstance = encoded;

    assertEquals(encoded, sameInstance);
  }

  @Test
  void encodedWithEqualBytesAndStatsAreEqual() {
    var encoded = new TileEncoder.Encoded(new byte[]{1, 2, 3}, POI_STATS);
    var sameContents = new TileEncoder.Encoded(new byte[]{1, 2, 3}, POI_STATS);

    assertEquals(encoded, sameContents);
    assertEquals(encoded.hashCode(), sameContents.hashCode());
  }

  @Test
  void encodedWithDifferentBytesAreNotEqual() {
    var encoded = new TileEncoder.Encoded(new byte[]{1, 2, 3}, POI_STATS);

    assertNotEquals(encoded, new TileEncoder.Encoded(new byte[]{1, 2, 4}, POI_STATS));
  }

  @Test
  void encodedWithDifferentLayerStatsAreNotEqual() {
    var encoded = new TileEncoder.Encoded(new byte[]{1, 2, 3}, POI_STATS);

    assertNotEquals(encoded, new TileEncoder.Encoded(new byte[]{1, 2, 3}, ROAD_STATS));
  }

  @Test
  void encodedIsNotEqualToOtherTypes() {
    var encoded = new TileEncoder.Encoded(new byte[]{1, 2, 3}, POI_STATS);

    assertNotEquals(encoded, new Object());
  }

  @Test
  void encodedToStringShowsTheByteCountInsteadOfTheBytes() {
    var encoded = new TileEncoder.Encoded(new byte[]{1, 2, 3}, POI_STATS);

    assertEquals("Encoded{bytes=byte[3], layerStats=" + POI_STATS + "}", encoded.toString());
  }
}
