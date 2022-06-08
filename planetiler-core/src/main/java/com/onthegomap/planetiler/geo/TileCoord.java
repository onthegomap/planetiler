package com.onthegomap.planetiler.geo;

import com.onthegomap.planetiler.mbtiles.Mbtiles;
import com.onthegomap.planetiler.util.Format;
import javax.annotation.concurrent.Immutable;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.CoordinateXY;

/**
 * The coordinate of a <a href="https://wiki.openstreetmap.org/wiki/Slippy_map_tilenames">slippy map tile</a>.
 * <p>
 * In order to encode into a 32-bit integer, we define a sequence of Hilbert curves for each zoom level, starting at the top-left.
 * <p>
 *
 * @param encoded the tile ID encoded as a 32-bit integer
 * @param x       x coordinate of the tile where 0 is the western-most tile just to the east the international date line
 *                and 2^z-1 is the eastern-most tile
 * @param y       y coordinate of the tile where 0 is the northern-most tile and 2^z-1 is the southern-most tile
 * @param z       zoom level ({@code <= 15})
 */
@Immutable
public record TileCoord(int encoded, int x, int y, int z) implements Comparable<TileCoord> {
  // z16+ would need more space

  private static final int XY_MASK = (1 << 14) - 1;

  public TileCoord {
    assert z <= 15;
  }

  public static TileCoord ofXYZ(int x, int y, int z) {
    return new TileCoord(encode(x, y, z), x, y, z);
  }

  public static TileCoord decode(int encoded) {
    int acc = 0;
    int tmp_z = 0;
    while (true) {
      int num_tiles = (1 << tmp_z) * (1 << tmp_z);
      if (acc + num_tiles > encoded) {
        int position = encoded - acc;
        return decodeOnLevel(tmp_z, position, encoded);
      }
      acc += num_tiles;
      tmp_z ++;
    }
  }

  private static void rotate(int n, int[] xy, int rx, int ry) {
    if (ry == 0) {
      if (rx == 1) {
        xy[0] = n - 1 - xy[0];
        xy[1] = n - 1 - xy[1];
      }
      int t = xy[0];
      xy[0] = xy[1];
      xy[1] = t;
    }
  }

  private static TileCoord decodeOnLevel(int z, int position, int encoded) {
    int n = 1 << z;
    int rx, ry, s, t = position;
    int[] xy = {0,0};
    for (s = 1; s < n; s *= 2) {
      rx = 1 & Integer.divideUnsigned(t,2);
      ry = 1 & (t ^ rx);
      rotate(s,xy,rx,ry);
      xy[0] += s * rx;
      xy[1] += s * ry;
      t = Integer.divideUnsigned(t,4);
    }
    return new TileCoord(encoded, xy[0],xy[1],z);
  }

  /** Returns the tile containing a latitude/longitude coordinate at a given zoom level. */
  public static TileCoord aroundLngLat(double lng, double lat, int zoom) {
    double factor = 1 << zoom;
    double x = GeoUtils.getWorldX(lng) * factor;
    double y = GeoUtils.getWorldY(lat) * factor;
    return TileCoord.ofXYZ((int) Math.floor(x), (int) Math.floor(y), zoom);
  }

  private static int encode(int x, int y, int z) {
    int acc = 0;
    for (int tmp_z = 0; tmp_z < z; tmp_z++) {
      acc += (1 << tmp_z) * (1 << tmp_z);
    }
    int n = 1 << z;
    int rx, ry, s, d = 0;
    int[] xy = {x,y};
    for (s = Integer.divideUnsigned(n,2); s > 0; s = Integer.divideUnsigned(s,2)) {
      rx = (xy[0] & s) > 0 ? 1 : 0;
      ry = (xy[1] & s) > 0 ? 1 : 0;
      d += s * s * ((3 * rx) ^ ry);
      rotate(s,xy,rx,ry);
    }
    return acc + d;
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

  @Override
  public String toString() {
    return "{x=" + x + " y=" + y + " z=" + z + '}';
  }

  @Override
  public int compareTo(TileCoord o) {
    return Long.compare(encoded, o.encoded);
  }

  /** Returns the latitude/longitude of the northwest corner of this tile. */
  public Coordinate getLatLon() {
    double worldWidthAtZoom = Math.pow(2, z);
    return new CoordinateXY(
      GeoUtils.getWorldLon(x / worldWidthAtZoom),
      GeoUtils.getWorldLat(y / worldWidthAtZoom)
    );
  }

  /** Returns a URL that displays the openstreetmap data for this tile. */
  public String getDebugUrl() {
    Coordinate coord = getLatLon();
    return Format.osmDebugUrl(z, coord);
  }

  /** Returns the pixel coordinate on this tile of a given latitude/longitude (assuming 256x256 px tiles). */
  public Coordinate lngLatToTileCoords(double lng, double lat) {
    double factor = 1 << z;
    double x = GeoUtils.getWorldX(lng) * factor;
    double y = GeoUtils.getWorldY(lat) * factor;
    return new CoordinateXY((x - Math.floor(x)) * 256, (y - Math.floor(y)) * 256);
  }
}
