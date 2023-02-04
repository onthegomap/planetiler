package com.onthegomap.planetiler.archive;

import com.onthegomap.planetiler.geo.TileCoord;
import com.onthegomap.planetiler.util.CloseableIterator;
import java.io.Closeable;

/**
 * Read API for on-disk representation of a tileset in a portable format. Example: MBTiles, a sqlite-based archive
 * format.
 * <p>
 * See {@link WriteableTileArchive} for the write API.
 */
public interface ReadableTileArchive extends Closeable {

  /** Returns the raw tile data associated with the tile at {@code coord}. */
  default byte[] getTile(TileCoord coord) {
    return getTile(coord.x(), coord.y(), coord.z());
  }

  /** Returns the raw tile data associated with the tile at coordinate {@code x, y, z}. */
  byte[] getTile(int x, int y, int z);

  /**
   * Returns an iterator over the coordinates of tiles in this archive.
   * <p>
   * The order should respect {@link WriteableTileArchive#tileOrder()} of the corresponding writer.
   * <p>
   * Clients should be sure to close the iterator after iterating through it, for example:
   *
   * <pre>
   * {@code
   * try (var iter = archive.getAllTileCoord()) {
   *   while (iter.hasNext()) {
   *     var coord = iter.next();
   *     ...
   *   }
   * }
   * }
   * </pre>
   */
  CloseableIterator<TileCoord> getAllTileCoords();

  // TODO access archive metadata
}
