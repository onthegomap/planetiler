package com.onthegomap.planetiler.osmmirror;

import com.onthegomap.planetiler.reader.osm.OsmElement;

public interface Serialized<T> {

  T item();

  byte[] bytes();

  record Node(OsmElement.Node item, byte[] bytes) implements Serialized<OsmElement.Node> {
    Node(OsmElement.Node item, boolean id) {
      this(item, OsmMirrorUtil.encode(item, id));
    }

    Node(byte[] item) {
      this(OsmMirrorUtil.decodeNode(item), item);
    }

    Node(byte[] item, long id) {
      this(OsmMirrorUtil.decodeNode(id, item), item);
    }
  }
  record Way(OsmElement.Way item, byte[] bytes) implements Serialized<OsmElement.Way> {
    Way(OsmElement.Way item, boolean id) {
      this(item, OsmMirrorUtil.encode(item, id));
    }

    Way(byte[] item) {
      this(OsmMirrorUtil.decodeWay(item), item);
    }

    Way(byte[] item, long id) {
      this(OsmMirrorUtil.decodeWay(id, item), item);
    }
  }
  record Relation(OsmElement.Relation item, byte[] bytes) implements Serialized<OsmElement.Relation> {
    Relation(OsmElement.Relation item, boolean id) {
      this(item, OsmMirrorUtil.encode(item, id));
    }

    Relation(byte[] item) {
      this(OsmMirrorUtil.decodeRelation(item), item);
    }

    Relation(byte[] item, long id) {
      this(OsmMirrorUtil.decodeRelation(id, item), item);
    }
  }
}
