package com.onthegomap.planetiler.reader.osm;

import java.io.Closeable;
import java.util.function.Consumer;

/**
 * An osm.pbf input file that iterates through {@link Block Blocks} of raw bytes that can be decompressed/parsed in
 * worker threads using {@link Block#decodeElements()}.
 */
public interface OsmBlockSource extends Closeable {

  /** Calls {@code consumer} for each block from the input file sequentially in a single thread. */
  void forEachBlock(Consumer<Block> consumer);

  @Override
  default void close() {
  }

  /**
   * An individual block of raw bytes from an osm.pbf file that can be decompressed/parsed with {@link
   * #decodeElements()}.
   */
  interface Block {

    /** Create a fake block from existing elements - useful for tests. */
    static Block of(Iterable<? extends OsmElement> items) {
      return () -> items;
    }

    /** Decompress and parse OSM elements from this block. */
    Iterable<? extends OsmElement> decodeElements();

    default int id() {
      return -1;
    }
  }
}
