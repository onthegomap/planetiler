package com.onthegomap.planetiler.reader.osm;

import java.io.Closeable;
import java.util.Iterator;
import java.util.function.Consumer;

/**
 * An osm.pbf input file that iterates through {@link Block Blocks} of raw bytes that can be decompressed/parsed in
 * worker threads using {@link Block#decodeElements()}.
 */
public interface OsmBlockSource extends Closeable {

  /** Calls {@code consumer} for each block from the input file sequentially in a single thread. */
  void forEachBlock(Consumer<Block> consumer);

  @Override
  default void close() {}

  /**
   * An individual block of raw bytes from an osm.pbf file that can be decompressed/parsed with
   * {@link #decodeElements()}.
   */
  interface Block extends Iterable<OsmElement> {

    /** Create a fake block from existing elements - useful for tests. */
    static <T extends OsmElement> Block of(Iterable<T> items) {
      return () -> {
        @SuppressWarnings("unchecked") Iterable<OsmElement> iterable = (Iterable<OsmElement>) items;
        return iterable;
      };
    }

    /** Decompress and parse OSM elements from this block. */
    Iterable<OsmElement> decodeElements();

    @Override
    default Iterator<OsmElement> iterator() {
      return decodeElements().iterator();
    }
  }
}
