package com.onthegomap.planetiler.writer;

import com.onthegomap.planetiler.config.PlanetilerConfig;
import com.onthegomap.planetiler.geo.TileOrder;
import com.onthegomap.planetiler.util.LayerStats;
import java.io.Closeable;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * A TileArchive is a on-disk representation of a tileset in a portable format. Example: MBTiles, a sqlite-based archive
 * format.
 */
@NotThreadSafe
public interface TileArchive extends Closeable {
  interface TileWriter extends Closeable {
    void write(TileEncodingResult encodingResult);

    // TODO: exists for compatibility reasons
    void write(com.onthegomap.planetiler.mbtiles.TileEncodingResult encodingResult);

    @Override
    void close();

    default void printStats() {}
  }


  /**
   * Specify the preferred insertion order for this archive, e.g. {@link TileOrder#TMS} or {@link TileOrder#HILBERT}.
   */
  TileOrder tileOrder();

  /**
   * Called before any tiles are written into {@link TileWriter}. Implementations of TileArchive should set up any
   * required state here.
   */
  void initialize(PlanetilerConfig config, TileArchiveMetadata metadata, LayerStats layerStats);

  /**
   * Implementations should return a object that implements {@link TileWriter} The specific TileWriter returned might
   * depend on {@link PlanetilerConfig}.
   */
  TileWriter newTileWriter();

  /**
   * Called after all tiles are written into {@link TileWriter}. After this is called, the archive should be complete on
   * disk.
   */
  void finish(PlanetilerConfig config);
}
