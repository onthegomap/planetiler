package com.onthegomap.flatmap.geo;

import java.text.NumberFormat;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.CoordinateXY;

public record TileCoord(int encoded, int x, int y, int z) implements Comparable<TileCoord> {

  public TileCoord {
    assert z <= 14;
  }

  public static TileCoord ofXYZ(int x, int y, int z) {
    return new TileCoord(encode(x, y, z), x, y, z);
  }

  private static final int XY_MASK = (1 << 14) - 1;

  public static TileCoord decode(int encoded) {
    int z = (encoded >> 28) + 8;
    int x = (encoded >> 14) & XY_MASK;
    int y = ((1 << z) - 1) - ((encoded) & XY_MASK);
    return new TileCoord(encoded, x, y, z);
  }

  public static TileCoord aroundLngLat(double lng, double lat, int zoom) {
    double factor = 1 << zoom;
    double x = GeoUtils.getWorldX(lng) * factor;
    double y = GeoUtils.getWorldY(lat) * factor;
    return TileCoord.ofXYZ((int) Math.floor(x), (int) Math.floor(y), zoom);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    TileCoord tileCoord = (TileCoord) o;

    return encoded == tileCoord.encoded;
  }

  @Override
  public int hashCode() {
    return encoded;
  }

  private static int encode(int x, int y, int z) {
    int max = 1 << z;
    if (x >= max) {
      x %= max;
    }
    if (x < 0) {
      x += max;
    }
    if (y < 0) {
      y = 0;
    }
    if (y >= max) {
      y = max - 1;
    }
    // since most significant bit is treated as the sign bit, make:
    // z0-7 get encoded from 8 (0b1000) to 15 (0b1111)
    // z8-14 get encoded from 0 (0b0000) to 6 (0b0110)
    // so that encoded tile coordinates are ordered by zoom level
    if (z < 8) {
      z += 8;
    } else {
      z -= 8;
    }
    y = max - 1 - y;
    return (z << 28) | (x << 14) | y;
  }

  @Override
  public String toString() {
    return "{x=" + x + " y=" + y + " z=" + z + '}';
  }

  @Override
  public int compareTo(TileCoord o) {
    return Long.compare(encoded, o.encoded);
  }

  public Coordinate getLatLon() {
    double worldWidthAtZoom = Math.pow(2, z);
    return new CoordinateXY(
      GeoUtils.getWorldLon(x / worldWidthAtZoom),
      GeoUtils.getWorldLat(y / worldWidthAtZoom)
    );
  }

  private static final NumberFormat format = NumberFormat.getNumberInstance();

  static {
    format.setMaximumFractionDigits(5);
  }

  public String getDebugUrl() {
    Coordinate coord = getLatLon();
    return "https://www.openstreetmap.org/#map=" + z + "/" + format.format(coord.y) + "/" + format.format(coord.x);
  }

  public Coordinate lngLatToTileCoords(double lng, double lat) {
    double factor = 1 << z;
    double x = GeoUtils.getWorldX(lng) * factor;
    double y = GeoUtils.getWorldY(lat) * factor;
    return new CoordinateXY((x - Math.floor(x)) * 256, (y - Math.floor(y)) * 256);
  }
}
