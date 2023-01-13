package com.onthegomap.planetiler.writer;

import com.onthegomap.planetiler.config.PlanetilerConfig;
import com.onthegomap.planetiler.util.LayerStats;

/**
 * A TileArchive is a portable on-disk stored tileset, e.g. MBTiles
 */
public interface TileArchive {

  /**
   * A high-throughput writer that accepts new tiles and queues up the writes to execute them in fewer large-batches.
   */
  public interface TileWriter extends AutoCloseable {
    void write(TileEncodingResult encodingResult);

    void write(com.onthegomap.planetiler.mbtiles.TileEncodingResult encodingResult);

    @Override
    void close();

    default void printStats() {}
  }

  void initialize(PlanetilerConfig config, TileArchiveMetadata metadata, LayerStats layerStats);

  TileWriter newTileWriter();

  void finish(PlanetilerConfig config);
}
