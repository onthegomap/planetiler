package com.onthegomap.planetiler.archive;

import com.onthegomap.planetiler.geo.TileCoord;
import com.onthegomap.planetiler.util.CloseableIterator;

/**
 * API for {@link ReadableTileArchive ReadableTileArchives} that have the ability to list their contents.
 */
public interface ScannableTileArchive {

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
}
