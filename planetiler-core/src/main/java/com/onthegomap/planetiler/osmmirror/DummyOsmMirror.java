package com.onthegomap.planetiler.osmmirror;

import com.carrotsearch.hppc.LongArrayList;
import com.onthegomap.planetiler.reader.osm.OsmElement;
import com.onthegomap.planetiler.util.CloseableIterator;
import java.io.IOException;
import org.geotools.data.store.EmptyIterator;

public class DummyOsmMirror implements OsmMirror {

  private class Bulk implements BulkWriter {

    @Override
    public void putNode(Serialized.Node node) {

    }

    @Override
    public void putWay(Serialized.Way way) {

    }

    @Override
    public void putRelation(Serialized.Relation node) {

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

  }

  @Override
  public void deleteWay(long wayId, int version) {

  }

  @Override
  public void deleteRelation(long relationId, int version) {

  }

  @Override
  public OsmElement.Node getNode(long id) {
    return null;
  }

  @Override
  public LongArrayList getParentWaysForNode(long nodeId) {
    return LongArrayList.from();
  }

  @Override
  public OsmElement.Way getWay(long id) {
    return null;
  }

  @Override
  public OsmElement.Relation getRelation(long id) {
    return null;
  }

  @Override
  public LongArrayList getParentRelationsForNode(long nodeId) {
    return LongArrayList.from();
  }

  @Override
  public LongArrayList getParentRelationsForWay(long nodeId) {
    return LongArrayList.from();
  }

  @Override
  public LongArrayList getParentRelationsForRelation(long nodeId) {
    return LongArrayList.from();
  }

  @Override
  public void close() throws Exception {

  }

  @Override
  public CloseableIterator<OsmElement> iterator() {
    return CloseableIterator.wrap(new EmptyIterator<>());
  }
}
