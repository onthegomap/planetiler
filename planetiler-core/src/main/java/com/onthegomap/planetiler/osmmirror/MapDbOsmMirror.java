package com.onthegomap.planetiler.osmmirror;

import com.carrotsearch.hppc.LongArrayList;
import com.google.common.collect.Iterators;
import com.onthegomap.planetiler.reader.osm.OsmElement;
import com.onthegomap.planetiler.util.CloseableIterator;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Path;
import java.util.NavigableSet;
import org.mapdb.BTreeKeySerializer;
import org.mapdb.BTreeMap;
import org.mapdb.Bind;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.Fun;
import org.mapdb.Serializer;

public class MapDbOsmMirror implements OsmMirror {

  private final BTreeMap<Long, Serialized.Node> nodes;
  private final BTreeMap<Long, Serialized.Way> ways;
  private final BTreeMap<Long, Serialized.Relation> relations;
  private NavigableSet<Fun.Tuple2<Long, Long>> waysByNode;
  private final NavigableSet<Fun.Tuple2<Long, Long>> relationsByWay;
  private final NavigableSet<Fun.Tuple2<Long, Long>> relationsByNode;
  private final NavigableSet<Fun.Tuple2<Long, Long>> relationsByRelation;
  private final DB db;

  static MapDbOsmMirror newInMemory() {
    return new MapDbOsmMirror(DBMaker.newMemoryDirectDB()
      .asyncWriteEnable()
      .transactionDisable()
      .closeOnJvmShutdown()
      .make());
  }

  static MapDbOsmMirror newWriteToFile(Path path) {
    return new MapDbOsmMirror(DBMaker
      .newFileDB(path.toFile())
      .asyncWriteEnable()
      .transactionDisable()
      .compressionEnable()
      .mmapFileEnableIfSupported()
      .closeOnJvmShutdown()
      .make());
  }

  static MapDbOsmMirror newReadFromFile(Path path) {
    return new MapDbOsmMirror(DBMaker
      .newFileDB(path.toFile())
      .transactionDisable()
      .compressionEnable()
      //.cacheLRUEnable()
      //.cacheSize(1000)
      .mmapFileEnableIfSupported()
      .make());
  }

  private static class NodeSerializer implements Serializer<Serialized.Node>, Serializable {

    @Override
    public void serialize(DataOutput out, Serialized.Node item) throws IOException {
      out.writeInt(item.bytes().length);
      out.write(item.bytes());
    }

    @Override
    public Serialized.Node deserialize(DataInput in, int available) throws IOException {
      int len = in.readInt();
      byte[] bytes = new byte[len];
      in.readFully(bytes);
      return new Serialized.Node(OsmMirrorUtil.decodeNode(bytes), bytes);
    }

    @Override
    public int fixedSize() {
      return -1;
    }
  }

  private static class WaySerializer implements Serializer<Serialized.Way>, Serializable {

    @Override
    public void serialize(DataOutput out, Serialized.Way item) throws IOException {
      out.writeInt(item.bytes().length);
      out.write(item.bytes());
    }

    @Override
    public Serialized.Way deserialize(DataInput in, int available) throws IOException {
      int len = in.readInt();
      byte[] bytes = new byte[len];
      in.readFully(bytes);
      return new Serialized.Way(OsmMirrorUtil.decodeWay(bytes), bytes);
    }

    @Override
    public int fixedSize() {
      return -1;
    }
  }

  private static class RelationSerializer implements Serializer<Serialized.Relation>, Serializable {

    @Override
    public void serialize(DataOutput out, Serialized.Relation item) throws IOException {
      out.writeInt(item.bytes().length);
      out.write(item.bytes());
    }

    @Override
    public Serialized.Relation deserialize(DataInput in, int available) throws IOException {
      int len = in.readInt();
      byte[] bytes = new byte[len];
      in.readFully(bytes);
      return new Serialized.Relation(OsmMirrorUtil.decodeRelation(bytes), bytes);
    }

    @Override
    public int fixedSize() {
      return -1;
    }
  }

  MapDbOsmMirror(DB db) {
    this.db = db;
    nodes = db.createTreeMap("nodes")
      .keySerializer(BTreeKeySerializer.ZERO_OR_POSITIVE_LONG)
      .valueSerializer(new NodeSerializer())
      .makeOrGet();

    ways = db.createTreeMap("ways")
      .keySerializer(BTreeKeySerializer.ZERO_OR_POSITIVE_LONG)
      .valueSerializer(new WaySerializer())
      .makeOrGet();

    relations = db.createTreeMap("relations")
      .keySerializer(BTreeKeySerializer.ZERO_OR_POSITIVE_LONG)
      .valueSerializer(new RelationSerializer())
      .makeOrGet();

    waysByNode = db.createTreeSet("ways_by_node")
      .serializer(BTreeKeySerializer.TUPLE2)
      .makeOrGet();

    Bind.secondaryKeys(ways, waysByNode,
      (k, w) -> {
        var nodes = w.item().nodes();
        Long[] result = new Long[nodes.size()];
        for (int i = 0; i < nodes.size(); i++) {
          result[i] = nodes.get(i);
        }
        return result;
      });

    relationsByWay = db.createTreeSet("relations_by_way")
      .serializer(BTreeKeySerializer.TUPLE2)
      .makeOrGet();

    relationsByNode = db.createTreeSet("relations_by_node")
      .serializer(BTreeKeySerializer.TUPLE2)
      .makeOrGet();

    relationsByRelation = db.createTreeSet("relations_by_relation")
      .serializer(BTreeKeySerializer.TUPLE2)
      .makeOrGet();

    Bind.secondaryKeys(relations, relationsByNode,
      (k, r) -> r.item().members().stream().filter(m -> m.type() == OsmElement.Type.NODE).map(m -> m.ref())
        .toArray(Long[]::new));

    Bind.secondaryKeys(relations, relationsByWay,
      (k, r) -> r.item().members().stream().filter(m -> m.type() == OsmElement.Type.WAY).map(m -> m.ref())
        .toArray(Long[]::new));

    Bind.secondaryKeys(relations, relationsByRelation,
      (k, r) -> r.item().members().stream().filter(m -> m.type() == OsmElement.Type.RELATION).map(m -> m.ref())
        .toArray(Long[]::new));
  }

  private class Bulk implements BulkWriter {

    @Override
    public void putNode(Serialized.Node node) {
      nodes.put(node.item().id(), node);
    }

    @Override
    public void putWay(Serialized.Way way) {
      ways.put(way.item().id(), way);
    }

    @Override
    public void putRelation(Serialized.Relation relation) {
      relations.put(relation.item().id(), relation);
    }

    @Override
    public void close() throws IOException {}
  }

  @Override
  public BulkWriter newBulkWriter() {
    return new Bulk();
  }

  @Override
  public void deleteNode(long nodeId, int version) {
    nodes.remove(nodeId);
  }

  @Override
  public void deleteWay(long wayId, int version) {
    ways.remove(wayId);
  }

  @Override
  public void deleteRelation(long relationId, int version) {
    relations.remove(relationId);
  }

  @Override
  public OsmElement.Node getNode(long id) {
    var got = nodes.get(id);
    return got == null ? null : got.item();
  }

  @Override
  public LongArrayList getParentWaysForNode(long nodeId) {
    var subset = waysByNode.subSet(new Fun.Tuple2<>(nodeId, 0L), new Fun.Tuple2<>(nodeId + 1, 0L));
    LongArrayList result = new LongArrayList();
    for (var entry : subset) {
      result.add(entry.b);
    }
    return result;
  }

  @Override
  public OsmElement.Way getWay(long id) {
    var got = ways.get(id);
    return got == null ? null : got.item();
  }

  @Override
  public OsmElement.Relation getRelation(long id) {
    var got = relations.get(id);
    return got == null ? null : got.item();
  }

  @Override
  public LongArrayList getParentRelationsForNode(long nodeId) {
    var subset = relationsByNode.subSet(new Fun.Tuple2<>(nodeId, 0L), new Fun.Tuple2<>(nodeId + 1, 0L));
    LongArrayList result = new LongArrayList();
    for (var entry : subset) {
      result.add(entry.b);
    }
    return result;
  }

  @Override
  public LongArrayList getParentRelationsForWay(long nodeId) {
    var subset = relationsByWay.subSet(new Fun.Tuple2<>(nodeId, 0L), new Fun.Tuple2<>(nodeId + 1, 0L));
    LongArrayList result = new LongArrayList();
    for (var entry : subset) {
      result.add(entry.b);
    }
    return result;
  }

  @Override
  public LongArrayList getParentRelationsForRelation(long nodeId) {
    var subset = relationsByRelation.subSet(new Fun.Tuple2<>(nodeId, 0L), new Fun.Tuple2<>(nodeId + 1, 0L));
    LongArrayList result = new LongArrayList();
    for (var entry : subset) {
      result.add(entry.b);
    }
    return result;
  }

  @Override
  public CloseableIterator<OsmElement> iterator() {
    return CloseableIterator.wrap(Iterators.concat(
      nodes.values().stream().map(d -> d.item()).iterator(),
      ways.values().stream().map(d -> d.item()).iterator(),
      relations.values().stream().map(d -> d.item()).iterator()
    ));
  }

  @Override
  public void close() throws Exception {
    db.close();
  }
}
