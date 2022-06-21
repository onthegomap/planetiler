package com.onthegomap.planetiler.geo;

import com.onthegomap.planetiler.util.Format;
import javax.annotation.concurrent.Immutable;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.CoordinateXY;

/**
 * The coordinate of a <a href="https://wiki.openstreetmap.org/wiki/Slippy_map_tilenames">slippy map tile</a>.
 * <p>
 * In order to encode into a 32-bit integer, we define a sequence of Hilbert curves for each zoom level, starting at the
 * top-left.
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

  public TileCoord {
    assert z <= 15;
  }

  public static TileCoord ofXYZ(int x, int y, int z) {
    return new TileCoord(encode(x, y, z), x, y, z);
  }

  public static TileCoord decode(int encoded) {
    int acc = 0;
    int tmpZ = 0;
    while (true) {
      int numTiles = (1 << tmpZ) * (1 << tmpZ);
      if (acc + numTiles > encoded) {
        int position = encoded - acc;
        long xy = hilbertIndexToXY(tmpZ, position);
        return new TileCoord(encoded, (int) (xy >>> 32 & 0xFFFFFFFFL), (int) (xy & 0xFFFFFFFFL), tmpZ);
      }
      acc += numTiles;
      tmpZ++;
    }
  }

  // Fast Hilbert curve algorithm by http://threadlocalmutex.com/
  // Ported from C++ https://github.com/rawrunprotected/hilbert_curves (public domain)
  private static int deinterleave(int tx) {
    tx = tx & 0x55555555;
    tx = (tx | (tx >>> 1)) & 0x33333333;
    tx = (tx | (tx >>> 2)) & 0x0F0F0F0F;
    tx = (tx | (tx >>> 4)) & 0x00FF00FF;
    tx = (tx | (tx >>> 8)) & 0x0000FFFF;
    return tx;
  }

  private static int interleave(int tx) {
    tx = (tx | (tx << 8)) & 0x00FF00FF;
    tx = (tx | (tx << 4)) & 0x0F0F0F0F;
    tx = (tx | (tx << 2)) & 0x33333333;
    tx = (tx | (tx << 1)) & 0x55555555;
    return tx;
  }

  private static int prefixScan(int tx) {
    tx = (tx >>> 8) ^ tx;
    tx = (tx >>> 4) ^ tx;
    tx = (tx >>> 2) ^ tx;
    tx = (tx >>> 1) ^ tx;
    return tx;
  }

  private static long hilbertIndexToXY(int n, int i) {
    i = i << (32 - 2 * n);

    int i0 = deinterleave(i);
    int i1 = deinterleave(i >>> 1);

    int t0 = (i0 | i1) ^ 0xFFFF;
    int t1 = i0 & i1;

    int prefixT0 = prefixScan(t0);
    int prefixT1 = prefixScan(t1);

    int a = (((i0 ^ 0xFFFF) & prefixT1) | (i0 & prefixT0));

    int resultX = (a ^ i1) >>> (16 - n);
    int resultY = (a ^ i0 ^ i1) >>> (16 - n);
    return ((long) resultX << 32) | resultY;
  }

  // Ignore warnings about nested scopes.
  @SuppressWarnings("java:S1199")
  private static int hilbertXYToIndex(int n, int tx, int ty) {
    tx = tx << (16 - n);
    ty = ty << (16 - n);

    int hA, hB, hC, hD;

    {
      int a = tx ^ ty;
      int b = 0xFFFF ^ a;
      int c = 0xFFFF ^ (tx | ty);
      int d = tx & (ty ^ 0xFFFF);

      hA = a | (b >>> 1);
      hB = (a >>> 1) ^ a;

      hC = ((c >>> 1) ^ (b & (d >>> 1))) ^ c;
      hD = ((a & (c >>> 1)) ^ (d >>> 1)) ^ d;
    }

    {
      int a = hA;
      int b = hB;
      int c = hC;
      int d = hD;

      hA = ((a & (a >>> 2)) ^ (b & (b >>> 2)));
      hB = ((a & (b >>> 2)) ^ (b & ((a ^ b) >>> 2)));

      hC ^= ((a & (c >>> 2)) ^ (b & (d >>> 2)));
      hD ^= ((b & (c >>> 2)) ^ ((a ^ b) & (d >>> 2)));
    }

    {
      int a = hA;
      int b = hB;
      int c = hC;
      int d = hD;

      hA = ((a & (a >>> 4)) ^ (b & (b >>> 4)));
      hB = ((a & (b >>> 4)) ^ (b & ((a ^ b) >>> 4)));

      hC ^= ((a & (c >>> 4)) ^ (b & (d >>> 4)));
      hD ^= ((b & (c >>> 4)) ^ ((a ^ b) & (d >>> 4)));
    }

    {
      int a = hA;
      int b = hB;
      int c = hC;
      int d = hD;

      hC ^= ((a & (c >>> 8)) ^ (b & (d >>> 8)));
      hD ^= ((b & (c >>> 8)) ^ ((a ^ b) & (d >>> 8)));
    }

    int a = hC ^ (hC >>> 1);
    int b = hD ^ (hD >>> 1);

    int i0 = tx ^ ty;
    int i1 = b | (0xFFFF ^ (i0 | a));

    return ((interleave(i1) << 1) | interleave(i0)) >>> (32 - 2 * n);
  }

  /** Returns the tile containing a latitude/longitude coordinate at a given zoom level. */
  public static TileCoord aroundLngLat(double lng, double lat, int zoom) {
    double factor = 1 << zoom;
    double x = GeoUtils.getWorldX(lng) * factor;
    double y = GeoUtils.getWorldY(lat) * factor;
    return TileCoord.ofXYZ((int) Math.floor(x), (int) Math.floor(y), zoom);
  }

  public static int encode(int x, int y, int z) {
    int acc = 0;
    for (int tmpZ = 0; tmpZ < z; tmpZ++) {
      acc += (1 << tmpZ) * (1 << tmpZ);
    }
    return acc + hilbertXYToIndex(z, x, y);
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
