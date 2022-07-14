package com.onthegomap.planetiler.geo;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.onthegomap.planetiler.TestUtils;
import org.junit.jupiter.api.Test;
import org.locationtech.jts.geom.Envelope;

class TileExtentsTest {

  private static final double eps = Math.pow(2, -30);

  @Test
  void testFullWorld() {
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
  void topLeft() {
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
  void topRight() {
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
  void testBottomLeft() {
    TileExtents extents = TileExtents
      .computeFromWorldBounds(14, new Envelope(0, eps, 1 - eps, 1));
    for (int z = 0; z <= 14; z++) {
      assertEquals(0, extents.getForZoom(z).minX(), "z" + z + " minX");
      assertEquals(1, extents.getForZoom(z).maxX(), "z" + z + " maxX");
      assertEquals((1 << z) - 1, extents.getForZoom(z).minY(), "z" + z + " minY");
      assertEquals(1 << z, extents.getForZoom(z).maxY(), "z" + z + " maxY");
    }
  }

  @Test
  void testShape() {
    var size = Math.pow(2, -14);
    var shape = GeoUtils.worldToLatLonCoords(TestUtils.newPolygon(
      0.5, 0.5 - size * 5,
      0.5 + size * 5, 0.5,
      0.5, 0.5 + size * 5,
      0.5 - size * 5, 0.5,
      0.5, 0.5 - size * 5
    ));
    TileExtents extents = TileExtents
      .computeFromWorldBounds(14, new Envelope(0.5 - size * 4, 0.5 + size * 4, 0.5 - size * 4, 0.5 + size * 4),
        shape);
    for (int z = 0; z <= 14; z++) {
      int middle = (1 << z) / 2;
      assertTrue(extents.test(middle, middle, z), "z" + z);
    }
    // inside shape and bounds
    assertTrue(extents.test((1 << 13) + 3, (1 << 13), 14));
    // inside shape but outside bounds
    assertFalse(extents.test((1 << 13) + 4, (1 << 13), 14));
    // inside bounds, outside shape
    assertFalse(extents.test((1 << 13) + 3, (1 << 13) + 3, 14));
  }
}
