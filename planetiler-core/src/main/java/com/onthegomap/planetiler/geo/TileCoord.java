package com.onthegomap.planetiler.geo;

import static com.onthegomap.planetiler.geo.GeoUtils.JTS_FACTORY;

import com.onthegomap.planetiler.mbtiles.Mbtiles;
import com.onthegomap.planetiler.util.Format;
import javax.annotation.concurrent.Immutable;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.CoordinateXY;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;

/**
 * The coordinate of a <a href="https://wiki.openstreetmap.org/wiki/Slippy_map_tilenames">slippy map tile</a>.
 * <p>
 * In order to encode into a 32-bit integer, only zoom levels {@code <= 14} are supported since we need 4 bits for the
 * zoom-level, and 14 bits each for the x/y coordinates.
 * <p>
 * Tiles are ordered by z ascending, x ascending, y descending to match index ordering of {@link Mbtiles} sqlite
 * database.
 *
 * @param encoded the tile ID encoded as a 32-bit integer
 * @param x       x coordinate of the tile where 0 is the western-most tile just to the east the international date line
 *                and 2^z-1 is the eastern-most tile
 * @param y       y coordinate of the tile where 0 is the northern-most tile and 2^z-1 is the southern-most tile
 * @param z       zoom level ({@code <= 14})
 */
@Immutable
public record TileCoord(int encoded, int x, int y, int z) implements Comparable<TileCoord> {
  // TODO: support higher than z14
  // z15 could theoretically fit into a 32-bit integer but needs a different packing strategy
  // z16+ would need more space
  // also need to remove hardcoded z14 limits

  private static final int XY_MASK = (1 << 14) - 1;

  public TileCoord {
    assert z <= 14;
  }

  public static TileCoord ofXYZ(int x, int y, int z) {
    return new TileCoord(encode(x, y, z), x, y, z);
  }

  public static TileCoord decode(int encoded) {
    int z = (encoded >> 28) + 8;
    int x = (encoded >> 14) & XY_MASK;
    int y = ((1 << z) - 1) - ((encoded) & XY_MASK);
    return new TileCoord(encoded, x, y, z);
  }

  /** Returns the tile containing a latitude/longitude coordinate at a given zoom level. */
  public static TileCoord aroundLngLat(double lng, double lat, int zoom) {
    double factor = 1 << zoom;
    double x = GeoUtils.getWorldX(lng) * factor;
    double y = GeoUtils.getWorldY(lat) * factor;
    return TileCoord.ofXYZ((int) Math.floor(x), (int) Math.floor(y), zoom);
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

  /** Returns the latitude/longitude of the northwest corner of this tile. */
  public Geometry getEnvelope() {
    double worldWidthAtZoom = Math.pow(2, z);
    return JTS_FACTORY.toGeometry(new Envelope(
      GeoUtils.getWorldLon(x / worldWidthAtZoom),
      GeoUtils.getWorldLon((x + 1) / worldWidthAtZoom),
      GeoUtils.getWorldLat(y / worldWidthAtZoom),
      GeoUtils.getWorldLat((y + 1) / worldWidthAtZoom)
    ));
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
