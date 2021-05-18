package com.onthegomap.flatmap;

import com.onthegomap.flatmap.geo.TileCoord;
import java.util.function.Predicate;
import org.locationtech.jts.geom.Envelope;

public class TileExtents implements Predicate<TileCoord> {

  private final ForZoom[] zoomExtents;

  private TileExtents(ForZoom[] zoomExtents) {
    this.zoomExtents = zoomExtents;
  }

  private static int quantizeDown(double value, int levels) {
    return Math.max(0, Math.min(levels, (int) Math.floor(value * levels)));
  }

  private static int quantizeUp(double value, int levels) {
    return Math.max(0, Math.min(levels, (int) Math.ceil(value * levels)));
  }

  public static TileExtents computeFromWorldBounds(int maxzoom, Envelope worldBounds) {
    ForZoom[] zoomExtents = new ForZoom[maxzoom + 1];
    for (int zoom = 0; zoom <= maxzoom; zoom++) {
      int max = 1 << zoom;
      zoomExtents[zoom] = new ForZoom(
        quantizeDown(worldBounds.getMinX(), max),
        quantizeDown(worldBounds.getMinY(), max),
        quantizeUp(worldBounds.getMaxX(), max),
        quantizeUp(worldBounds.getMaxY(), max)
      );
    }
    return new TileExtents(zoomExtents);
  }

  public ForZoom getForZoom(int zoom) {
    return zoomExtents[zoom];
  }

  public boolean test(int x, int y, int z) {
    return getForZoom(z).test(x, y);
  }

  @Override
  public boolean test(TileCoord tileCoord) {
    return test(tileCoord.x(), tileCoord.y(), tileCoord.z());
  }

  public record ForZoom(int minX, int minY, int maxX, int maxY) {

    public boolean test(int x, int y) {
      return testX(x) && testY(y);
    }

    public boolean testX(int x) {
      return x >= minX && x <= maxX;
    }

    public boolean testY(int y) {
      return y >= minY && y <= maxY;
    }
  }
}
