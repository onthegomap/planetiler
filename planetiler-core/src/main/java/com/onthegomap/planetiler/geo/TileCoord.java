package com.onthegomap.planetiler.geo;

import com.onthegomap.planetiler.util.Format;
import javax.annotation.concurrent.Immutable;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.CoordinateXY;

/**
 * The coordinate of a <a href="https://wiki.openstreetmap.org/wiki/Slippy_map_tilenames">slippy map tile</a>.
 * <p>
 * Tile coords are sorted by consecutive Z levels in ascending order: 0 coords for z=0, 4 coords for z=1, etc. TMS
 * order: tiles in a level are sorted by x ascending, y descending to match the ordering of {@link mbtiles.Mbtiles}
 * sqlite index. Hilbert order: tiles in a level are ordered on the Hilbert curve with the first coordinate at the tip
 * left.
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
        // long xy = hilbertPositionToXY(tmpZ, position);
        long xy = tmsPositionToXY(tmpZ, position);
        return new TileCoord(encoded, (int) (xy >>> 32 & 0xFFFFFFFFL), (int) (xy & 0xFFFFFFFFL), tmpZ);
      }
      acc += numTiles;
      tmpZ++;
    }
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
    // return acc + hilbertXYToPosition(z, x, y);
    return acc + tmsXYToPosition(z, x, y);
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

  public double progressOnLevel() {
    int acc = 0;
    int tmpZ = 0;
    while (true) {
      int numTiles = (1 << tmpZ) * (1 << tmpZ);
      if (acc + numTiles > encoded) {
        return (encoded - acc) / (double) numTiles;
      }
      acc += numTiles;
      tmpZ++;
    }
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

  public static long tmsPositionToXY(int z, int pos) {
    if (z == 0)
      return 0;
    int dim = 1 << z;
    int x = pos / dim;
    int y = dim - 1 - (pos % dim);
    return ((long) x << 32) | y;
  }

  public static int tmsXYToPosition(int z, int x, int y) {
    int dim = 1 << z;
    return x * dim + (dim - 1 - y);
  }

  // hilbert implementation (not currently used)
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

  private static long hilbertPositionToXY(int z, int pos) {
    pos = pos << (32 - 2 * z);

    int i0 = deinterleave(pos);
    int i1 = deinterleave(pos >>> 1);

    int t0 = (i0 | i1) ^ 0xFFFF;
    int t1 = i0 & i1;

    int prefixT0 = prefixScan(t0);
    int prefixT1 = prefixScan(t1);

    int a = (((i0 ^ 0xFFFF) & prefixT1) | (i0 & prefixT0));

    int resultX = (a ^ i1) >>> (16 - z);
    int resultY = (a ^ i0 ^ i1) >>> (16 - z);
    return ((long) resultX << 32) | resultY;
  }

  private static int hilbertXYToIndex(int z, int x, int y) {
    x = x << (16 - z);
    y = y << (16 - z);

    int hA, hB, hC, hD;

    int a1 = x ^ y;
    int b1 = 0xFFFF ^ a1;
    int c1 = 0xFFFF ^ (x | y);
    int d1 = x & (y ^ 0xFFFF);

    hA = a1 | (b1 >>> 1);
    hB = (a1 >>> 1) ^ a1;

    hC = ((c1 >>> 1) ^ (b1 & (d1 >>> 1))) ^ c1;
    hD = ((a1 & (c1 >>> 1)) ^ (d1 >>> 1)) ^ d1;

    int a2 = hA;
    int b2 = hB;
    int c2 = hC;
    int d2 = hD;

    hA = ((a2 & (a2 >>> 2)) ^ (b2 & (b2 >>> 2)));
    hB = ((a2 & (b2 >>> 2)) ^ (b2 & ((a2 ^ b2) >>> 2)));

    hC ^= ((a2 & (c2 >>> 2)) ^ (b2 & (d2 >>> 2)));
    hD ^= ((b2 & (c2 >>> 2)) ^ ((a2 ^ b2) & (d2 >>> 2)));

    int a3 = hA;
    int b3 = hB;
    int c3 = hC;
    int d3 = hD;

    hA = ((a3 & (a3 >>> 4)) ^ (b3 & (b3 >>> 4)));
    hB = ((a3 & (b3 >>> 4)) ^ (b3 & ((a3 ^ b3) >>> 4)));

    hC ^= ((a3 & (c3 >>> 4)) ^ (b3 & (d3 >>> 4)));
    hD ^= ((b3 & (c3 >>> 4)) ^ ((a3 ^ b3) & (d3 >>> 4)));

    int a4 = hA;
    int b4 = hB;
    int c4 = hC;
    int d4 = hD;

    hC ^= ((a4 & (c4 >>> 8)) ^ (b4 & (d4 >>> 8)));
    hD ^= ((b4 & (c4 >>> 8)) ^ ((a4 ^ b4) & (d4 >>> 8)));

    int a = hC ^ (hC >>> 1);
    int b = hD ^ (hD >>> 1);

    int i0 = x ^ y;
    int i1 = b | (0xFFFF ^ (i0 | a));

    return ((interleave(i1) << 1) | interleave(i0)) >>> (32 - 2 * z);
  }
}
