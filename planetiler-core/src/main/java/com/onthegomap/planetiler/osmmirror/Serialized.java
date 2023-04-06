package com.onthegomap.planetiler.osmmirror;

import com.onthegomap.planetiler.reader.osm.OsmElement;

public interface Serialized<T> {

  T item();

  byte[] bytes();

  record SerializedNode(OsmElement.Node item, byte[] bytes) implements Serialized<OsmElement.Node> {}
  record SerializedWay(OsmElement.Way item, byte[] bytes) implements Serialized<OsmElement.Way> {}
  record SerializedRelation(OsmElement.Relation item, byte[] bytes) implements Serialized<OsmElement.Relation> {}
}
