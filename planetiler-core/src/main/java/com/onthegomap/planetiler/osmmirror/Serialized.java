package com.onthegomap.planetiler.osmmirror;

import com.onthegomap.planetiler.reader.osm.OsmElement;

public interface Serialized<T> {

  T item();

  byte[] bytes();

  record Node(OsmElement.Node item, byte[] bytes) implements Serialized<OsmElement.Node> {}
  record Way(OsmElement.Way item, byte[] bytes) implements Serialized<OsmElement.Way> {}
  record Relation(OsmElement.Relation item, byte[] bytes) implements Serialized<OsmElement.Relation> {}
}
