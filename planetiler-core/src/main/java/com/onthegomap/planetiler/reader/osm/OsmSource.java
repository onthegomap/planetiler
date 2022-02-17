package com.onthegomap.planetiler.reader.osm;

import com.onthegomap.planetiler.worker.WorkerPipeline;

public interface OsmSource extends AutoCloseable {

  /**
   * Returns a source that initiates a {@link WorkerPipeline} with raw OSM elements.
   *
   * @return work for a source thread
   */
  WorkerPipeline.SourceStep<Block> readBlocks();

  @Override
  default void close() {
  }

  interface Block {

    static Block of(Iterable<? extends OsmElement> items) {
      return () -> items;
    }

    Iterable<? extends OsmElement> parse();
  }
}
