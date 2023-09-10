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

  /** Returns the raw tile data at {@code coord} or {@code null} if not found. */
  default byte[] getTile(TileCoord coord) {
    return getTile(coord.x(), coord.y(), coord.z());
  }

  /** Returns the raw tile data at {@code x, y, z} or {@code null} if not found. */
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
   * try (var iter = archive.getAllTileCoords()) {
   *   while (iter.hasNext()) {
   *     var coord = iter.next();
   *     ...
   *   }
   * }
   * }
   * </pre>
   */
  CloseableIterator<TileCoord> getAllTileCoords();

  default CloseableIterator<Tile> getAllTiles() {
    return new CloseableIterator<>() {
      private final CloseableIterator<TileCoord> coords = getAllTileCoords();

      @Override
      public boolean hasNext() {
        return coords.hasNext();
      }

      @Override
      public Tile next() {
        var coord = coords.next();
        return new Tile(coord, getTile(coord));
      }

      @Override
      public void close() {
        coords.close();
      }
    };
  }

  /**
   * Returns the metadata stored in this archive.
   */
  TileArchiveMetadata metadata();

  // TODO access archive metadata
}
