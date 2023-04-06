package com.onthegomap.planetiler.osmmirror;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import com.carrotsearch.hppc.LongArrayList;
import com.onthegomap.planetiler.reader.osm.OsmElement;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

abstract class OsmMirrorTest {
  OsmMirror fixture;

  @Test
  void testEmpty() {
    assertNull(fixture.getNode(1));
    assertNull(fixture.getWay(1));
    assertNull(fixture.getRelation(1));
    assertEquals(LongArrayList.from(), fixture.getParentWaysForNode(1));
    assertEquals(LongArrayList.from(), fixture.getParentRelationsForNode(1));
    assertEquals(LongArrayList.from(), fixture.getParentRelationsForWay(1));
    assertEquals(LongArrayList.from(), fixture.getParentRelationsForRelation(1));
    assertEquals(List.of(), fixture.iterator().toList());
  }

  @Test
  void testInsertNode() throws IOException {
    var node = new OsmElement.Node(1, Map.of("key", "value"), 2, 3, infoVersion(0));
    try (var writer = fixture.newBulkWriter()) {
      writer.putNode(node);
    }
    assertEquals(node, fixture.getNode(1));
    assertEquals(List.of(node), fixture.iterator().toList());
  }

  @Test
  void testInsertWay() throws IOException {
    var way = new OsmElement.Way(2, Map.of("key", "value"), LongArrayList.from(1), infoVersion(0));
    var way2 = new OsmElement.Way(3, Map.of("key", "value"), LongArrayList.from(1), infoVersion(0));
    try (var writer = fixture.newBulkWriter()) {
      writer.putWay(way);
      writer.putWay(way2);
    }
    assertEquals(way, fixture.getWay(2));
    assertEquals(way2, fixture.getWay(3));
    assertEquals(List.of(way, way2), fixture.iterator().toList());
    assertEquals(LongArrayList.from(2, 3), fixture.getParentWaysForNode(1));
  }

  @Test
  void testInsertRelation() throws IOException {
    var rel = new OsmElement.Relation(3, Map.of("key", "value"), List.of(
      new OsmElement.Relation.Member(OsmElement.Type.NODE, 1, "node")
    ), infoVersion(0));
    var rel2 = new OsmElement.Relation(4, Map.of("key", "value"), List.of(
      new OsmElement.Relation.Member(OsmElement.Type.NODE, 1, "node"),
      new OsmElement.Relation.Member(OsmElement.Type.WAY, 2, "way"),
      new OsmElement.Relation.Member(OsmElement.Type.RELATION, 3, "rel")
    ), infoVersion(0));
    try (var writer = fixture.newBulkWriter()) {
      writer.putRelation(rel);
      writer.putRelation(rel2);
    }
    assertEquals(rel, fixture.getRelation(3));
    assertEquals(rel2, fixture.getRelation(4));
    assertEquals(List.of(rel, rel2), fixture.iterator().toList());
    assertEquals(LongArrayList.from(3, 4), fixture.getParentRelationsForNode(1));
    assertEquals(LongArrayList.from(4), fixture.getParentRelationsForWay(2));
    assertEquals(LongArrayList.from(4), fixture.getParentRelationsForRelation(3));
  }

  private OsmElement.Info infoVersion(int version) {
    return OsmElement.Info.forVersion(version);
  }

  static class InMemoryTest extends OsmMirrorTest {
    @BeforeEach
    void setup() {
      fixture = new InMemoryOsmMirror();
    }
  }
  static class SqliteTest extends OsmMirrorTest {

    @BeforeEach
    void setup() {
      fixture = OsmMirror.newSqliteMemory();
    }
  }
  static class MapdbTest extends OsmMirrorTest {

    @BeforeEach
    void setup() {
      fixture = OsmMirror.newMapdbMemory();
    }
  }
  static class MapdbFileTest extends OsmMirrorTest {
    @TempDir
    Path tmpDir;

    @BeforeEach
    void setup() {
      fixture = OsmMirror.newMapdbWrite(tmpDir.resolve("db"));
    }
  }
  static class LmdbTest extends OsmMirrorTest {
    @TempDir
    Path tmp;

    @BeforeEach
    void setup() {
      fixture = OsmMirror.newLmdbWrite(tmp);
    }
  }
}
