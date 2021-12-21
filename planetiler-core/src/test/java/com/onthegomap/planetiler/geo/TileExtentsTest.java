package com.onthegomap.planetiler.geo;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;
import org.locationtech.jts.geom.Envelope;

public class TileExtentsTest {

  private static final double eps = Math.pow(2, -30);

  @Test
  public void testFullWorld() {
    TileExtents extents = TileExtents.computeFromWorldBounds(14, GeoUtils.WORLD_BOUNDS);
    for (int z = 0; z <= 14; z++) {
      int max = 1 << z;
      assertEquals(0, extents.getForZoom(z).minX(), "z" + z + " minX");
      assertEquals(max, extents.getForZoom(z).maxX(), "z" + z + " maxX");
      assertEquals(0, extents.getForZoom(z).minY(), "z" + z + " minY");
      assertEquals(max, extents.getForZoom(z).maxY(), "z" + z + " maxY");
    }
  }

  @Test
  public void topLeft() {
    TileExtents extents = TileExtents
      .computeFromWorldBounds(14, new Envelope(0, eps, 0, eps));
    for (int z = 0; z <= 14; z++) {
      assertEquals(0, extents.getForZoom(z).minX(), "z" + z + " minX");
      assertEquals(1, extents.getForZoom(z).maxX(), "z" + z + " maxX");
      assertEquals(0, extents.getForZoom(z).minY(), "z" + z + " minY");
      assertEquals(1, extents.getForZoom(z).maxY(), "z" + z + " maxY");
    }
  }

  @Test
  public void topRight() {
    TileExtents extents = TileExtents
      .computeFromWorldBounds(14, new Envelope(1 - eps, 1, 0, eps));
    for (int z = 0; z <= 14; z++) {
      assertEquals((1 << z) - 1, extents.getForZoom(z).minX(), "z" + z + " minX");
      assertEquals((1 << z), extents.getForZoom(z).maxX(), "z" + z + " maxX");
      assertEquals(0, extents.getForZoom(z).minY(), "z" + z + " minY");
      assertEquals(1, extents.getForZoom(z).maxY(), "z" + z + " maxY");
    }
  }

  @Test
  public void testBottomLeft() {
    TileExtents extents = TileExtents
      .computeFromWorldBounds(14, new Envelope(0, eps, 1 - eps, 1));
    for (int z = 0; z <= 14; z++) {
      assertEquals(0, extents.getForZoom(z).minX(), "z" + z + " minX");
      assertEquals(1, extents.getForZoom(z).maxX(), "z" + z + " maxX");
      assertEquals((1 << z) - 1, extents.getForZoom(z).minY(), "z" + z + " minY");
      assertEquals(1 << z, extents.getForZoom(z).maxY(), "z" + z + " maxY");
    }
  }
}
