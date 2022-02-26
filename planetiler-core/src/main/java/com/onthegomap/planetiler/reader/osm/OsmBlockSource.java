package com.onthegomap.planetiler.reader.osm;

import java.io.Closeable;
import java.util.function.Consumer;

public interface OsmBlockSource extends Closeable {

  void forEachBlock(Consumer<Block> consumer);

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
