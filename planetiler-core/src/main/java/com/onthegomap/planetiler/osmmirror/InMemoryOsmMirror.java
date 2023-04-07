package com.onthegomap.planetiler.osmmirror;

import com.carrotsearch.hppc.LongArrayList;
import com.google.common.collect.Iterators;
import com.onthegomap.planetiler.reader.osm.OsmElement;
import com.onthegomap.planetiler.util.CloseableIterator;
import java.io.IOException;
import java.util.NavigableMap;
import java.util.TreeMap;

public class InMemoryOsmMirror implements OsmMirror {
  private final NavigableMap<Long, OsmElement.Node> nodes = new TreeMap<>();
  private final NavigableMap<Long, OsmElement.Way> ways = new TreeMap<>();
  private final NavigableMap<Long, LongArrayList> nodesToParentWay = new TreeMap<>();
  private final NavigableMap<Long, OsmElement.Relation> relations = new TreeMap<>();
  private final NavigableMap<Long, LongArrayList> nodesToParentRelation = new TreeMap<>();
  private final NavigableMap<Long, LongArrayList> waysToParentRelation = new TreeMap<>();
  private final NavigableMap<Long, LongArrayList> relationsToParentRelation = new TreeMap<>();


  private class Bulk implements BulkWriter {
    @Override
    public void putNode(Serialized.Node node) {
      nodes.put(node.item().id(), node.item());
    }

    @Override
    public void putWay(Serialized.Way way) {
      var previous = ways.put(way.item().id(), way.item());
      if (previous != null) {
        for (var node : previous.nodes()) {
          nodesToParentWay.get(node.value).removeAll(way.item().id());
        }
      }
      for (var node : way.item().nodes()) {
        nodesToParentWay.computeIfAbsent(node.value, c -> new LongArrayList()).add(way.item().id());
      }
    }

    @Override
    public void putRelation(Serialized.Relation relation) {
      var previous = relations.put(relation.item().id(), relation.item());
      if (previous != null) {
        for (var member : previous.members()) {
          (switch (member.type()) {
            case NODE -> nodesToParentRelation;
            case WAY -> waysToParentRelation;
            case RELATION -> relationsToParentRelation;
          }).get(member.ref()).removeAll(relation.item().id());
        }
      }
      for (var member : relation.item().members()) {
        (switch (member.type()) {
          case NODE -> nodesToParentRelation;
          case WAY -> waysToParentRelation;
          case RELATION -> relationsToParentRelation;
        }).computeIfAbsent(member.ref(), c -> new LongArrayList()).add(relation.item().id());
      }
    }

    @Override
    public void close() throws IOException {

    }
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
    ways.remove(relationId);
  }

  @Override
  public OsmElement.Node getNode(long id) {
    return nodes.get(id);
  }

  @Override
  public LongArrayList getParentWaysForNode(long nodeId) {
    return nodesToParentWay.getOrDefault(nodeId, LongArrayList.from());
  }

  @Override
  public OsmElement.Way getWay(long id) {
    return ways.get(id);
  }

  @Override
  public OsmElement.Relation getRelation(long id) {
    return relations.get(id);
  }

  @Override
  public LongArrayList getParentRelationsForNode(long nodeId) {
    return nodesToParentRelation.getOrDefault(nodeId, LongArrayList.from());
  }

  @Override
  public LongArrayList getParentRelationsForWay(long nodeId) {
    return waysToParentRelation.getOrDefault(nodeId, LongArrayList.from());
  }

  @Override
  public LongArrayList getParentRelationsForRelation(long nodeId) {
    return relationsToParentRelation.getOrDefault(nodeId, LongArrayList.from());
  }

  @Override
  public CloseableIterator<OsmElement> iterator() {
    return CloseableIterator.wrap(Iterators.concat(
      nodes.values().iterator(),
      ways.values().iterator(),
      relations.values().iterator()
    ));
  }

  @Override
  public void close() throws Exception {}
}
