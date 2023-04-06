package com.onthegomap.planetiler.osmmirror;

import static org.lmdbjava.DbiFlags.MDB_CREATE;
import static org.lmdbjava.DbiFlags.MDB_INTEGERKEY;

import com.carrotsearch.hppc.LongArrayList;
import com.google.common.collect.Iterators;
import com.onthegomap.planetiler.reader.osm.OsmElement;
import com.onthegomap.planetiler.util.CloseableIterator;
import com.onthegomap.planetiler.util.FileUtils;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.function.Function;
import org.lmdbjava.ByteArrayProxy;
import org.lmdbjava.Dbi;
import org.lmdbjava.Env;
import org.lmdbjava.KeyRange;
import org.lmdbjava.Txn;

public class LmdbOsmMirror implements OsmMirror {
  private final Dbi<byte[]> nodes;
  private final Dbi<byte[]> ways;
  private final Dbi<byte[]> relations;
  private final Dbi<byte[]> waysByNode;
  private final Dbi<byte[]> relationsByWay;
  private final Dbi<byte[]> relationsByNode;
  private final Dbi<byte[]> relationsByRelation;
  private final Env<byte[]> env;

  public LmdbOsmMirror(Path file) {
    if (!Files.exists(file)) {
      FileUtils.createDirectory(file);
    }
    env = Env.create(ByteArrayProxy.PROXY_BA)
      // LMDB also needs to know how large our DB might be. Over-estimating is OK.
      .setMapSize(5_000_000_000L)
      // LMDB also needs to know how many DBs (Dbi) we want to store in this Env.
      .setMaxDbs(7)
      // Now let's open the Env. The same path can be concurrently opened and
      // used in different processes, but do not open the same path twice in
      // the same process at the same time.
      .open(file.toFile());

    nodes = env.openDbi("nodes", MDB_CREATE, MDB_INTEGERKEY);
    ways = env.openDbi("ways", MDB_CREATE, MDB_INTEGERKEY);
    relations = env.openDbi("relations", MDB_CREATE, MDB_INTEGERKEY);
    waysByNode = env.openDbi("waysByNode", MDB_CREATE, MDB_INTEGERKEY);
    relationsByWay = env.openDbi("relationsByWay", MDB_CREATE, MDB_INTEGERKEY);
    relationsByNode = env.openDbi("relationsByNode", MDB_CREATE, MDB_INTEGERKEY);
    relationsByRelation = env.openDbi("relationsByRelation", MDB_CREATE, MDB_INTEGERKEY);
  }

  private static byte[] key(long id) {
    return ByteBuffer.allocate(8).order(ByteOrder.nativeOrder()).putLong(id).flip().array();
  }

  private static byte[] key(long id1, long id2) {
    return ByteBuffer.allocate(16).order(ByteOrder.nativeOrder()).putLong(id1).putLong(id2).flip().array();
  }

  private static long getKeyPart2(byte[] key) {
    return ByteBuffer.wrap(key).order(ByteOrder.nativeOrder()).getLong(8);
  }

  private static <I, O> Iterator<O> mapIter(Iterator<I> iter, Function<I, O> fn) {
    return new Iterator<>() {
      @Override
      public boolean hasNext() {
        return iter.hasNext();
      }

      @Override
      public O next() {
        return fn.apply(iter.next());
      }
    };
  }

  @Override
  public BulkWriter newBulkWriter() {
    return new Bulk();
  }

  @Override
  public void deleteNode(long nodeId, int version) {
    ways.delete(key(nodeId));
  }

  @Override
  public void deleteWay(long wayId, int version) {
    ways.delete(key(wayId));
  }

  @Override
  public void deleteRelation(long relationId, int version) {
    ways.delete(key(relationId));
  }

  @Override
  public OsmElement.Node getNode(long id) {
    try (var txn = env.txnRead()) {
      byte[] value = nodes.get(txn, key(id));
      return value == null ? null : OsmMirrorUtil.decodeNode(value);
    }
  }

  @Override
  public LongArrayList getParentWaysForNode(long nodeId) {
    return scanKeyPart2(nodeId, waysByNode);
  }

  @Override
  public OsmElement.Way getWay(long id) {
    try (var txn = env.txnRead()) {
      byte[] value = ways.get(txn, key(id));
      return value == null ? null : OsmMirrorUtil.decodeWay(value);
    }
  }

  @Override
  public OsmElement.Relation getRelation(long id) {
    try (var txn = env.txnRead()) {
      byte[] value = relations.get(txn, key(id));
      return value == null ? null : OsmMirrorUtil.decodeRelation(value);
    }
  }

  @Override
  public LongArrayList getParentRelationsForNode(long nodeId) {
    var toScan = relationsByNode;
    return scanKeyPart2(nodeId, toScan);
  }

  private LongArrayList scanKeyPart2(long keyPart1, Dbi<byte[]> toScan) {
    try (
      var txn = env.txnRead();
      var cursor = toScan.iterate(txn, KeyRange.closedOpen(key(keyPart1, 0), key(keyPart1 + 1, 0)));
    ) {
      LongArrayList result = new LongArrayList();
      for (var item : cursor) {
        result.add(getKeyPart2(item.key()));
      }
      return result;
    }
  }

  @Override
  public LongArrayList getParentRelationsForWay(long nodeId) {
    return scanKeyPart2(nodeId, relationsByWay);
  }

  @Override
  public LongArrayList getParentRelationsForRelation(long nodeId) {
    return scanKeyPart2(nodeId, relationsByRelation);
  }

  @Override
  public CloseableIterator<OsmElement> iterator() {
    var txn = env.txnRead();
    var nodeIter = nodes.iterate(txn);
    var wayIter = ways.iterate(txn);
    var relIter = relations.iterate(txn);

    Iterator<OsmElement> iter = Iterators.concat(
      mapIter(nodeIter.iterator(), d -> OsmMirrorUtil.decodeNode(d.val())),
      mapIter(wayIter.iterator(), d -> OsmMirrorUtil.decodeWay(d.val())),
      mapIter(relIter.iterator(), d -> OsmMirrorUtil.decodeRelation(d.val()))
    );

    return new CloseableIterator<>() {
      @Override
      public void close() {
        txn.close();
        try (nodeIter; wayIter; relIter; txn) {
        }
      }

      @Override
      public boolean hasNext() {
        return iter.hasNext();
      }

      @Override
      public OsmElement next() {
        return iter.next();
      }
    };
  }

  @Override
  public void close() throws Exception {

  }

  private class Bulk implements BulkWriter {
    private static final byte[] EMPTY_BYTE = new byte[0];
    private Txn<byte[]> txn = env.txnWrite();
    int size = 0;

    private void maybeCommit() {
      if (size++ > 10_000) {
        txn.commit();
        txn.close();
        txn = env.txnWrite();
        size = 0;
      }
    }

    @Override
    public void putNode(Serialized.SerializedNode node) {
      nodes.put(txn, key(node.item().id()), node.bytes());
      maybeCommit();
    }

    @Override
    public void putWay(Serialized.SerializedWay way) {
      ways.put(txn, key(way.item().id()), way.bytes());
      for (var node : way.item().nodes()) {
        waysByNode.put(txn, key(node.value, way.item().id()), EMPTY_BYTE);
      }
      maybeCommit();
    }

    @Override
    public void putRelation(Serialized.SerializedRelation relation) {
      relations.put(txn, key(relation.item().id()), relation.bytes());
      for (var member : relation.item().members()) {
        (switch (member.type()) {
          case NODE -> relationsByNode;
          case WAY -> relationsByWay;
          case RELATION -> relationsByRelation;
        }).put(txn, key(member.ref(), relation.item().id()), EMPTY_BYTE);
      }
      maybeCommit();
    }

    @Override
    public void close() throws IOException {
      txn.commit();
      txn.close();
    }
  }
}
