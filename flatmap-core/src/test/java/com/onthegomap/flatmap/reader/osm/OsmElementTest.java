package com.onthegomap.flatmap.reader.osm;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.carrotsearch.hppc.LongArrayList;
import com.graphhopper.reader.ReaderNode;
import com.graphhopper.reader.ReaderRelation;
import com.graphhopper.reader.ReaderWay;
import com.graphhopper.reader.osm.OSMFileHeader;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

public class OsmElementTest {

  @Test
  public void testFileheader() {
    OSMFileHeader header = new OSMFileHeader();
    assertEquals(new OsmElement.Other(0, Map.of()), OsmElement.fromGraphhopper(header));
  }

  @Test
  public void testNode() {
    ReaderNode input = new ReaderNode(1, 2, 3);
    input.setTag("a", "b");
    OsmElement.Node node = (OsmElement.Node) OsmElement.fromGraphhopper(input);
    assertEquals(1, node.id());
    assertEquals(2, node.lat());
    assertEquals(3, node.lon());
    assertEquals(Map.of("a", "b"), node.tags());
  }

  @Test
  public void testWay() {
    ReaderWay input = new ReaderWay(1);
    input.setTag("a", "b");
    input.getNodes().add(1, 2, 3);
    OsmElement.Way way = (OsmElement.Way) OsmElement.fromGraphhopper(input);
    assertEquals(1, way.id());
    assertEquals(LongArrayList.from(1, 2, 3), way.nodes());
    assertEquals(Map.of("a", "b"), way.tags());
  }

  @Test
  public void testRelation() {
    ReaderRelation input = new ReaderRelation(1);
    input.setTag("a", "b");
    input.add(new ReaderRelation.Member(ReaderRelation.Member.NODE, 1, "noderole"));
    input.add(new ReaderRelation.Member(ReaderRelation.Member.WAY, 2, "wayrole"));
    input.add(new ReaderRelation.Member(ReaderRelation.Member.RELATION, 3, "relationrole"));
    OsmElement.Relation relation = (OsmElement.Relation) OsmElement.fromGraphhopper(input);
    assertEquals(1, relation.id());
    assertEquals(Map.of("a", "b"), relation.tags());
    assertEquals(List.of(
      new OsmElement.Relation.Member(OsmElement.Type.NODE, 1, "noderole"),
      new OsmElement.Relation.Member(OsmElement.Type.WAY, 2, "wayrole"),
      new OsmElement.Relation.Member(OsmElement.Type.RELATION, 3, "relationrole")
    ), relation.members());
  }
}
