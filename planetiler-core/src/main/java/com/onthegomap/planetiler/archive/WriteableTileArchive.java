package com.onthegomap.planetiler.archive;

import com.onthegomap.planetiler.config.PlanetilerConfig;
import com.onthegomap.planetiler.geo.TileOrder;
import java.io.Closeable;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * Write API for an on-disk representation of a tileset in a portable format. Example: MBTiles, a sqlite-based archive
 * format.
 * <p>
 * See {@link ReadableTileArchive} for the read API.
 */
@NotThreadSafe
public interface WriteableTileArchive extends Closeable {

  /**
   * Returns true if this tile archive deduplicates tiles with the same content.
   * <p>
   * If false, then {@link TileWriter} will skip computing tile hashes.
   */
  boolean deduplicates();

  /**
   * Specify the preferred insertion order for this archive, e.g. {@link TileOrder#TMS} or {@link TileOrder#HILBERT}.
   */
  TileOrder tileOrder();

  /**
   * Called before any tiles are written into {@link TileWriter}. Implementations of TileArchive should set up any
   * required state here.
   */
  default void initialize() {}

  /**
   * Implementations should return a object that implements {@link TileWriter} The specific TileWriter returned might
   * depend on {@link PlanetilerConfig}.
   */
  TileWriter newTileWriter();

  /**
   * Called after all tiles are written into {@link TileWriter}. After this is called, the archive should be complete on
   * disk.
   */
  default void finish(TileArchiveMetadata tileArchiveMetadata) {}

  default void printStats() {}

  long bytesWritten();

  interface TileWriter extends Closeable {

    void write(TileEncodingResult encodingResult);

    @Override
    void close();

    default void printStats() {}
  }

  // TODO update archive metadata
}
