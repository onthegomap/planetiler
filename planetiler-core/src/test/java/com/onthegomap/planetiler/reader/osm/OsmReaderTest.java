package com.onthegomap.planetiler.reader.osm;

import static com.onthegomap.planetiler.TestUtils.*;
import static org.junit.jupiter.api.Assertions.*;

import com.carrotsearch.hppc.LongArrayList;
import com.onthegomap.planetiler.Profile;
import com.onthegomap.planetiler.TestUtils;
import com.onthegomap.planetiler.collection.LongLongMap;
import com.onthegomap.planetiler.collection.LongLongMultimap;
import com.onthegomap.planetiler.geo.GeoUtils;
import com.onthegomap.planetiler.geo.GeometryException;
import com.onthegomap.planetiler.reader.SourceFeature;
import com.onthegomap.planetiler.stats.Stats;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class OsmReaderTest {

  public final OsmBlockSource osmSource = next -> {
  };
  private final Stats stats = Stats.inMemory();
  private final Profile profile = new Profile.NullProfile();
  private final LongLongMap nodeMap = LongLongMap.newInMemorySortedTable();
  private final LongLongMultimap.Replaceable multipolygons = LongLongMultimap.newInMemoryReplaceableMultimap();

  private void processPass1Block(OsmReader reader, Iterable<OsmElement> block) {
    reader.processPass1Blocks(List.of(block));
  }

  @Test
  void testPoint() throws GeometryException {
    OsmReader reader = newOsmReader();
    var node = new OsmElement.Node(1, 0, 0);
    node.setTag("key", "value");
    processPass1Block(reader, List.of(node));
    SourceFeature feature = reader.processNodePass2(node);
    assertTrue(feature.isPoint());
    assertFalse(feature.canBePolygon());
    assertFalse(feature.canBeLine());
    assertSameNormalizedFeature(
      newPoint(0.5, 0.5),
      feature.worldGeometry(),
      feature.centroid(),
      feature.pointOnSurface(),
      GeoUtils.latLonToWorldCoords(feature.latLonGeometry())
    );
    assertEquals(0, feature.area());
    assertEquals(0, feature.length());
    assertThrows(GeometryException.class, feature::line);
    assertThrows(GeometryException.class, feature::polygon);
    assertEquals(Map.of("key", "value"), feature.tags());
  }

  @Test
  void testLine() throws GeometryException {
    OsmReader reader = newOsmReader();
    var nodeCache = reader.newNodeLocationProvider();
    var node1 = new OsmElement.Node(1, 0, 0);
    var node2 = node(2, 0.75, 0.75);
    var way = new OsmElement.Way(3);
    way.nodes().add(node1.id(), node2.id());
    way.setTag("key", "value");

    processPass1Block(reader, List.of(node1, node2, way));

    SourceFeature feature = reader.processWayPass2(way, nodeCache);
    assertTrue(feature.canBeLine());
    assertFalse(feature.isPoint());
    assertFalse(feature.canBePolygon());

    assertSameNormalizedFeature(
      newLineString(
        0.5, 0.5,
        0.75, 0.75
      ),
      feature.worldGeometry(),
      feature.line(),
      GeoUtils.latLonToWorldCoords(feature.latLonGeometry())
    );
    assertThrows(GeometryException.class, feature::polygon);
    assertEquals(
      newPoint(0.625, 0.625),
      feature.centroid()
    );
    assertPointOnSurface(feature);

    assertEquals(0, feature.area());
    assertEquals(Math.sqrt(2 * 0.25 * 0.25), feature.length(), 1e-5);
    assertEquals(Map.of("key", "value"), feature.tags());
  }

  @Test
  void testPolygonAreaNotSpecified() throws GeometryException {
    OsmReader reader = newOsmReader();
    var nodeCache = reader.newNodeLocationProvider();
    var node1 = node(1, 0.5, 0.5);
    var node2 = node(2, 0.5, 0.75);
    var node3 = node(3, 0.75, 0.75);
    var node4 = node(4, 0.75, 0.5);
    var way = new OsmElement.Way(3);
    way.nodes().add(1, 2, 3, 4, 1);
    way.setTag("key", "value");

    processPass1Block(reader, List.of(node1, node2, node3, node4, way));

    SourceFeature feature = reader.processWayPass2(way, nodeCache);
    assertTrue(feature.canBeLine());
    assertFalse(feature.isPoint());
    assertTrue(feature.canBePolygon());

    assertSameNormalizedFeature(
      rectangle(0.5, 0.75),
      feature.worldGeometry(),
      feature.polygon(),
      GeoUtils.latLonToWorldCoords(feature.latLonGeometry())
    );
    assertSameNormalizedFeature(
      rectangle(0.5, 0.75).getExteriorRing(),
      feature.line()
    );
    assertEquals(
      newPoint(0.625, 0.625),
      feature.centroid()
    );
    assertPointOnSurface(feature);

    assertEquals(0.25 * 0.25, feature.area());
    assertEquals(1, feature.length());
  }

  @Test
  void testPolygonAreaYes() throws GeometryException {
    OsmReader reader = newOsmReader();
    var nodeCache = reader.newNodeLocationProvider();
    var node1 = node(1, 0.5, 0.5);
    var node2 = node(2, 0.5, 0.75);
    var node3 = node(3, 0.75, 0.75);
    var node4 = node(4, 0.75, 0.5);
    var way = new OsmElement.Way(3);
    way.nodes().add(1, 2, 3, 4, 1);
    way.setTag("area", "yes");

    processPass1Block(reader, List.of(node1, node2, node3, node4, way));

    SourceFeature feature = reader.processWayPass2(way, nodeCache);
    assertFalse(feature.canBeLine());
    assertFalse(feature.isPoint());
    assertTrue(feature.canBePolygon());

    assertSameNormalizedFeature(
      rectangle(0.5, 0.75),
      feature.worldGeometry(),
      feature.polygon(),
      GeoUtils.latLonToWorldCoords(feature.latLonGeometry())
    );
    assertThrows(GeometryException.class, feature::line);
    assertEquals(
      newPoint(0.625, 0.625),
      feature.centroid()
    );
    assertPointOnSurface(feature);

    assertEquals(0.25 * 0.25, feature.area());
    assertEquals(1, feature.length());
  }

  @Test
  void testPolygonAreaNo() throws GeometryException {
    OsmReader reader = newOsmReader();
    var nodeCache = reader.newNodeLocationProvider();
    var node1 = node(1, 0.5, 0.5);
    var node2 = node(2, 0.5, 0.75);
    var node3 = node(3, 0.75, 0.75);
    var node4 = node(4, 0.75, 0.5);
    var way = new OsmElement.Way(5);
    way.nodes().add(1, 2, 3, 4, 1);
    way.setTag("area", "no");

    processPass1Block(reader, List.of(node1, node2, node3, node4, way));

    SourceFeature feature = reader.processWayPass2(way, nodeCache);
    assertTrue(feature.canBeLine());
    assertFalse(feature.isPoint());
    assertFalse(feature.canBePolygon());

    assertSameNormalizedFeature(
      rectangle(0.5, 0.75).getExteriorRing(),
      feature.worldGeometry(),
      feature.line(),
      GeoUtils.latLonToWorldCoords(feature.latLonGeometry())
    );
    assertThrows(GeometryException.class, feature::polygon);
    assertEquals(
      newPoint(0.625, 0.625),
      feature.centroid()
    );
    assertPointOnSurface(feature);

    assertEquals(0, feature.area());
    assertEquals(1, feature.length());
  }

  @Test
  void testLineWithTooFewPoints() throws GeometryException {
    OsmReader reader = newOsmReader();
    var node1 = node(1, 0.5, 0.5);
    var way = new OsmElement.Way(3);
    way.nodes().add(1);

    processPass1Block(reader, List.of(node1, way));

    SourceFeature feature = reader.processWayPass2(way, reader.newNodeLocationProvider());
    assertFalse(feature.canBeLine());
    assertFalse(feature.isPoint());
    assertFalse(feature.canBePolygon());

    assertThrows(GeometryException.class, feature::worldGeometry);
    assertThrows(GeometryException.class, feature::latLonGeometry);
    assertThrows(GeometryException.class, feature::line);
    assertThrows(GeometryException.class, feature::centroid);
    assertThrows(GeometryException.class, feature::pointOnSurface);

    assertEquals(0, feature.area());
    assertEquals(0, feature.length());
  }

  @Test
  void testPolygonWithTooFewPoints() throws GeometryException {
    OsmReader reader = newOsmReader();
    var node1 = node(1, 0.5, 0.5);
    var node2 = node(2, 0.5, 0.75);
    var way = new OsmElement.Way(3);
    way.nodes().add(1, 2, 1);

    processPass1Block(reader, List.of(node1, node2, way));

    SourceFeature feature = reader.processWayPass2(way, reader.newNodeLocationProvider());
    assertTrue(feature.canBeLine());
    assertFalse(feature.isPoint());
    assertFalse(feature.canBePolygon());

    assertSameNormalizedFeature(
      newLineString(0.5, 0.5, 0.5, 0.75, 0.5, 0.5),
      feature.worldGeometry(),
      feature.line(),
      GeoUtils.latLonToWorldCoords(feature.latLonGeometry())
    );
    assertSameNormalizedFeature(
      newPoint(0.5, 0.625),
      feature.centroid()
    );
    assertPointOnSurface(feature);

    assertEquals(0, feature.area());
    assertEquals(0.5, feature.length());
  }

  private static void assertPointOnSurface(SourceFeature feature) throws GeometryException {
    TestUtils.assertPointOnSurface(feature.worldGeometry(), feature.pointOnSurface());
  }

  @Test
  void testInvalidPolygon() throws GeometryException {
    OsmReader reader = newOsmReader();

    processPass1Block(reader, List.of(
      node(1, 0.5, 0.5),
      node(2, 0.75, 0.5),
      node(3, 0.5, 0.75),
      node(4, 0.75, 0.75)
    ));
    var way = new OsmElement.Way(6);
    way.setTag("area", "yes");
    way.nodes().add(1, 2, 3, 4, 1);
    processPass1Block(reader, List.of(way));

    SourceFeature feature = reader.processWayPass2(way, reader.newNodeLocationProvider());
    assertFalse(feature.canBeLine());
    assertFalse(feature.isPoint());
    assertTrue(feature.canBePolygon());

    assertSameNormalizedFeature(
      newPolygon(
        0.5, 0.5,
        0.75, 0.5,
        0.5, 0.75,
        0.75, 0.75,
        0.5, 0.5
      ),
      feature.worldGeometry(),
      GeoUtils.latLonToWorldCoords(feature.latLonGeometry())
    );
    assertThrows(GeometryException.class, feature::line);
    assertSameNormalizedFeature(
      newPoint(0.625, 0.625),
      feature.centroid()
    );
    assertPointOnSurface(feature);

    assertEquals(0, feature.area());
    assertEquals(1.207, feature.length(), 1e-2);
  }

  private OsmElement.Node node(long id, double x, double y) {
    return new OsmElement.Node(id, GeoUtils.getWorldLat(y), GeoUtils.getWorldLon(x));
  }

  @Test
  void testLineReferencingNonexistentNode() {
    OsmReader reader = newOsmReader();
    var way = new OsmElement.Way(321);
    way.nodes().add(123, 2222, 333, 444, 123);
    processPass1Block(reader, List.of(way));

    SourceFeature feature = reader.processWayPass2(way, reader.newNodeLocationProvider());
    assertTrue(feature.canBeLine());
    assertFalse(feature.isPoint());
    assertTrue(feature.canBePolygon());

    GeometryException exception = assertThrows(GeometryException.class, feature::line);
    assertTrue(exception.getMessage().contains("321") && exception.getMessage().contains("123"),
      "Exception message did not contain way and missing node ID: " + exception.getMessage()
    );
    assertThrows(GeometryException.class, feature::worldGeometry);
    assertThrows(GeometryException.class, feature::centroid);
    assertThrows(GeometryException.class, feature::polygon);
    assertThrows(GeometryException.class, feature::pointOnSurface);
    assertThrows(GeometryException.class, feature::area);
    assertThrows(GeometryException.class, feature::length);
  }

  private final Function<OsmElement, Stream<OsmElement.Node>> nodes =
    elem -> elem instanceof OsmElement.Node node ? Stream.of(node) : Stream.empty();

  private final Function<OsmElement, Stream<OsmElement.Way>> ways =
    elem -> elem instanceof OsmElement.Way way ? Stream.of(way) : Stream.empty();

  @ParameterizedTest
  @ValueSource(strings = {"multipolygon", "boundary", "land_area"})
  void testMultiPolygon(String relationType) throws GeometryException {
    OsmReader reader = newOsmReader();
    var outerway = new OsmElement.Way(9);
    outerway.nodes().add(1, 2, 3, 4, 1);
    var innerway = new OsmElement.Way(10);
    innerway.nodes().add(5, 6, 7, 8, 5);

    var relation = new OsmElement.Relation(11);
    relation.setTag("type", relationType);
    relation.members().add(new OsmElement.Relation.Member(OsmElement.Type.WAY, outerway.id(), "outer"));
    relation.members().add(new OsmElement.Relation.Member(OsmElement.Type.WAY, innerway.id(), "inner"));

    List<OsmElement> elements = List.of(
      node(1, 0.1, 0.1),
      node(2, 0.9, 0.1),
      node(3, 0.9, 0.9),
      node(4, 0.1, 0.9),

      node(5, 0.2, 0.2),
      node(6, 0.8, 0.2),
      node(7, 0.8, 0.8),
      node(8, 0.2, 0.8),

      outerway,
      innerway,

      relation
    );

    processPass1Block(reader, elements);
    elements.stream().flatMap(nodes).forEach(reader::processNodePass2);
    var nodeCache = reader.newNodeLocationProvider();
    elements.stream().flatMap(ways).forEach(way -> reader.processWayPass2(way, nodeCache));

    var feature = reader.processRelationPass2(relation, nodeCache);

    assertFalse(feature.canBeLine());
    assertFalse(feature.isPoint());
    assertTrue(feature.canBePolygon());

    assertSameNormalizedFeature(
      newPolygon(
        rectangleCoordList(0.1, 0.9),
        List.of(rectangleCoordList(0.2, 0.8))
      ),
      round(feature.worldGeometry()),
      round(feature.polygon()),
      round(feature.validatedPolygon()),
      round(GeoUtils.latLonToWorldCoords(feature.latLonGeometry()))
    );
    assertThrows(GeometryException.class, feature::line);
    assertSameNormalizedFeature(
      newPoint(0.5, 0.5),
      round(feature.centroid())
    );
    assertPointOnSurface(feature);

    assertEquals(0.28, feature.area(), 1e-5);
    assertEquals(5.6, feature.length(), 1e-2);
  }

  @Test
  void testMultipolygonInfersCorrectParent() throws GeometryException {
    OsmReader reader = newOsmReader();
    var outerway = new OsmElement.Way(13);
    outerway.nodes().add(1, 2, 3, 4, 1);
    var innerway = new OsmElement.Way(14);
    innerway.nodes().add(5, 6, 7, 8, 5);
    var innerinnerway = new OsmElement.Way(15);
    innerinnerway.nodes().add(9, 10, 11, 12, 9);

    var relation = new OsmElement.Relation(16);
    relation.setTag("type", "multipolygon");
    relation.members().add(new OsmElement.Relation.Member(OsmElement.Type.WAY, outerway.id(), "outer"));
    relation.members().add(new OsmElement.Relation.Member(OsmElement.Type.WAY, innerway.id(), "inner"));
    // nested hole marked as inner, but should actually be outer
    relation.members().add(new OsmElement.Relation.Member(OsmElement.Type.WAY, innerinnerway.id(), "inner"));

    List<OsmElement> elements = List.of(
      node(1, 0.1, 0.1),
      node(2, 0.9, 0.1),
      node(3, 0.9, 0.9),
      node(4, 0.1, 0.9),

      node(5, 0.2, 0.2),
      node(6, 0.8, 0.2),
      node(7, 0.8, 0.8),
      node(8, 0.2, 0.8),

      node(9, 0.3, 0.3),
      node(10, 0.7, 0.3),
      node(11, 0.7, 0.7),
      node(12, 0.3, 0.7),

      outerway,
      innerway,
      innerinnerway,

      relation
    );

    processPass1Block(reader, elements);
    elements.stream().flatMap(nodes).forEach(reader::processNodePass2);
    var nodeCache = reader.newNodeLocationProvider();
    elements.stream().flatMap(ways).forEach(way -> reader.processWayPass2(way, nodeCache));

    var feature = reader.processRelationPass2(relation, nodeCache);

    assertFalse(feature.canBeLine());
    assertFalse(feature.isPoint());
    assertTrue(feature.canBePolygon());

    assertSameNormalizedFeature(
      newMultiPolygon(
        newPolygon(
          rectangleCoordList(0.1, 0.9),
          List.of(rectangleCoordList(0.2, 0.8))
        ),
        rectangle(0.3, 0.7)
      ),
      round(feature.worldGeometry()),
      round(feature.polygon()),
      round(feature.validatedPolygon()),
      round(GeoUtils.latLonToWorldCoords(feature.latLonGeometry()))
    );
  }

  @Test
  void testInvalidMultipolygon() throws GeometryException {
    OsmReader reader = newOsmReader();
    var outerway = new OsmElement.Way(13);
    outerway.nodes().add(1, 2, 3, 4, 1);
    var innerway = new OsmElement.Way(14);
    innerway.nodes().add(5, 6, 7, 8, 5);
    var innerinnerway = new OsmElement.Way(15);
    innerinnerway.nodes().add(9, 10, 11, 12, 9);

    var relation = new OsmElement.Relation(16);
    relation.setTag("type", "multipolygon");
    relation.members().add(new OsmElement.Relation.Member(OsmElement.Type.WAY, outerway.id(), "outer"));
    relation.members().add(new OsmElement.Relation.Member(OsmElement.Type.WAY, innerway.id(), "inner"));
    // nested hole marked as inner, but should actually be outer
    relation.members().add(new OsmElement.Relation.Member(OsmElement.Type.WAY, innerinnerway.id(), "inner"));

    List<OsmElement> elements = List.of(
      node(1, 0.1, 0.1),
      node(2, 0.9, 0.1),
      node(3, 0.9, 0.9),
      node(4, 0.1, 0.9),

      node(5, 0.2, 0.3),
      node(6, 0.8, 0.3),
      node(7, 0.8, 0.8),
      node(8, 0.2, 0.8),

      node(9, 0.2, 0.2),
      node(10, 0.8, 0.2),
      node(11, 0.8, 0.7),
      node(12, 0.2, 0.7),

      outerway,
      innerway,
      innerinnerway,

      relation
    );

    processPass1Block(reader, elements);
    elements.stream().flatMap(nodes).forEach(reader::processNodePass2);
    var nodeCache = reader.newNodeLocationProvider();
    elements.stream().flatMap(ways).forEach(way -> reader.processWayPass2(way, nodeCache));

    var feature = reader.processRelationPass2(relation, nodeCache);

    assertFalse(feature.canBeLine());
    assertFalse(feature.isPoint());
    assertTrue(feature.canBePolygon());

    assertTopologicallyEquivalentFeature(
      newPolygon(
        rectangleCoordList(0.1, 0.9),
        List.of(rectangleCoordList(0.2, 0.8))
      ),
      round(feature.validatedPolygon())
    );
    assertSameNormalizedFeature(
      newPolygon(
        rectangleCoordList(0.1, 0.9),
        List.of(
          rectangleCoordList(0.2, 0.3, 0.8, 0.8),
          rectangleCoordList(0.2, 0.2, 0.8, 0.7)
        )
      ),
      round(feature.polygon())
    );
  }


  @Test
  void testMultipolygonInvalidMembers() {
    OsmReader reader = newOsmReader();

    var relation = new OsmElement.Relation(16);

    var childRelation = new OsmElement.Relation(17);
    var childNode = new OsmElement.Node(17, 0.0, 0.0);

    relation.setTag("type", "multipolygon");
    relation.members().add(new OsmElement.Relation.Member(OsmElement.Type.RELATION, childRelation.id(), "outer"));
    relation.members().add(new OsmElement.Relation.Member(OsmElement.Type.NODE, childNode.id(), "inner"));

    List<OsmElement> elements = List.of(
      node(1, 0.1, 0.1),
      node(2, 0.9, 0.1),
      node(3, 0.9, 0.9),
      node(4, 0.1, 0.9),

      node(5, 0.2, 0.3),
      node(6, 0.8, 0.3),
      node(7, 0.8, 0.8),
      node(8, 0.2, 0.8),

      node(9, 0.2, 0.2),
      node(10, 0.8, 0.2),
      node(11, 0.8, 0.7),
      node(12, 0.2, 0.7),

      childNode,
      childRelation,

      relation
    );

    processPass1Block(reader, elements);
    elements.stream().flatMap(nodes).forEach(reader::processNodePass2);
    var nodeCache = reader.newNodeLocationProvider();
    elements.stream().flatMap(ways).forEach(way -> reader.processWayPass2(way, nodeCache));

    var feature = reader.processRelationPass2(relation, nodeCache);

    assertNull(feature);
  }

  @Test
  void testMultiPolygonRefersToNonexistentNode() {
    OsmReader reader = newOsmReader();
    var outerway = new OsmElement.Way(5);
    outerway.nodes().add(1, 2, 3, 4, 1);

    var relation = new OsmElement.Relation(6);
    relation.setTag("type", "multipolygon");
    relation.members().add(new OsmElement.Relation.Member(OsmElement.Type.WAY, outerway.id(), "outer"));

    List<OsmElement> elements = List.of(
      node(1, 0.1, 0.1),
      //      node(2, 0.9, 0.1), MISSING!
      node(3, 0.9, 0.9),
      node(4, 0.1, 0.9),

      outerway,

      relation
    );

    processPass1Block(reader, elements);
    elements.stream().flatMap(nodes).forEach(reader::processNodePass2);
    var nodeCache = reader.newNodeLocationProvider();
    elements.stream().flatMap(ways).forEach(way -> reader.processWayPass2(way, nodeCache));

    var feature = reader.processRelationPass2(relation, nodeCache);

    assertThrows(GeometryException.class, feature::worldGeometry);
    assertThrows(GeometryException.class, feature::polygon);
    assertThrows(GeometryException.class, feature::validatedPolygon);
  }

  @Test
  void testMultiPolygonRefersToNonexistentWay() {
    OsmReader reader = newOsmReader();

    var relation = new OsmElement.Relation(6);
    relation.setTag("type", "multipolygon");
    relation.members().add(new OsmElement.Relation.Member(OsmElement.Type.WAY, 5, "outer"));

    List<OsmElement> elements = List.of(
      node(1, 0.1, 0.1),
      node(2, 0.9, 0.1),
      node(3, 0.9, 0.9),
      node(4, 0.1, 0.9),

      //      outerway, // missing!

      relation
    );

    processPass1Block(reader, elements);
    elements.stream().flatMap(nodes).forEach(reader::processNodePass2);
    var nodeCache = reader.newNodeLocationProvider();
    elements.stream().flatMap(ways).forEach(way -> reader.processWayPass2(way, nodeCache));

    var feature = reader.processRelationPass2(relation, nodeCache);

    assertThrows(GeometryException.class, feature::worldGeometry);
    assertThrows(GeometryException.class, feature::polygon);
    assertThrows(GeometryException.class, feature::validatedPolygon);
  }

  @Test
  void testWayInRelation() {
    record OtherRelInfo(long id) implements OsmRelationInfo {}
    record TestRelInfo(long id, String name) implements OsmRelationInfo {}
    OsmReader reader = new OsmReader("osm", () -> osmSource, nodeMap, multipolygons, new Profile.NullProfile() {
      @Override
      public List<OsmRelationInfo> preprocessOsmRelation(OsmElement.Relation relation) {
        return List.of(new TestRelInfo(1, "name"));
      }
    }, stats);
    var nodeCache = reader.newNodeLocationProvider();
    var node1 = new OsmElement.Node(1, 0, 0);
    var node2 = node(2, 0.75, 0.75);
    var way = new OsmElement.Way(3);
    way.nodes().add(node1.id(), node2.id());
    way.setTag("key", "value");
    var relation = new OsmElement.Relation(4);
    relation.members().add(new OsmElement.Relation.Member(OsmElement.Type.WAY, 3, "rolename"));

    processPass1Block(reader, List.of(node1, node2, way, relation));

    SourceFeature feature = reader.processWayPass2(way, nodeCache);

    assertEquals(List.of(), feature.relationInfo(OtherRelInfo.class));
    assertEquals(List.of(new OsmReader.RelationMember<>("rolename", new TestRelInfo(1, "name"), List.of())),
      feature.relationInfo(TestRelInfo.class));
  }

  @Test
  void testNodeOrWayRelationInRelationDoesntTriggerWay() {
    record TestRelInfo(long id, String name) implements OsmRelationInfo {}
    OsmReader reader = new OsmReader("osm", () -> osmSource, nodeMap, multipolygons, new Profile.NullProfile() {
      @Override
      public List<OsmRelationInfo> preprocessOsmRelation(OsmElement.Relation relation) {
        return List.of(new TestRelInfo(1, "name"));
      }
    }, stats);
    var nodeCache = reader.newNodeLocationProvider();
    var node1 = new OsmElement.Node(1, 0, 0);
    var node2 = node(2, 0.75, 0.75);
    var way = new OsmElement.Way(3);
    way.nodes().add(node1.id(), node2.id());
    way.setTag("key", "value");
    var relation = new OsmElement.Relation(4);
    relation.members().add(new OsmElement.Relation.Member(OsmElement.Type.RELATION, 3, "rolename"));
    relation.members().add(new OsmElement.Relation.Member(OsmElement.Type.NODE, 3, "rolename"));

    processPass1Block(reader, List.of(node1, node2, way, relation));

    SourceFeature feature = reader.processWayPass2(way, nodeCache);

    assertEquals(List.of(), feature.relationInfo(TestRelInfo.class));
  }

  @Test
  void testSuperRelationNotOptedIn() {
    record TestRelInfo(long id, String name) implements OsmRelationInfo {}
    var testRelInfo = new TestRelInfo(1, "name");
    OsmReader reader = new OsmReader("osm", () -> osmSource, nodeMap, multipolygons, new Profile.NullProfile() {
      @Override
      public List<OsmRelationInfo> preprocessOsmRelation(OsmElement.Relation relation) {
        return List.of(testRelInfo);
      }
    }, stats);
    var nodeCache = reader.newNodeLocationProvider();
    long index = 1;
    var node1 = node(index++, 0, 0);
    var node2 = node(index++, 1, 1);
    var node3 = node(index++, 2, 2);
    var node4 = node(index++, 3, 3);
    var way1 = new OsmElement.Way(index++);
    way1.nodes().add(node1.id(), node2.id());
    way1.setTag("key", "value");
    var way2 = new OsmElement.Way(index++);
    way2.nodes().add(node3.id(), node4.id());
    way2.setTag("key", "value");
    var relation1 = new OsmElement.Relation(index++);
    var relation2 = new OsmElement.Relation(index++);
    var relation3 = new OsmElement.Relation(index);
    relation1.members().add(new OsmElement.Relation.Member(OsmElement.Type.WAY, way1.id(), "leafway"));

    relation2.members().add(new OsmElement.Relation.Member(OsmElement.Type.RELATION, relation1.id(), "midrel"));
    relation2.members().add(new OsmElement.Relation.Member(OsmElement.Type.WAY, way2.id(), "rolename"));

    relation3.members().add(new OsmElement.Relation.Member(OsmElement.Type.RELATION, relation1.id(), "topmosrel"));

    processPass1Block(reader, List.of(node1, node2, node3, node4, way1, way2, relation1, relation2, relation3));

    SourceFeature feature1 = reader.processWayPass2(way1, nodeCache);
    SourceFeature feature2 = reader.processWayPass2(way2, nodeCache);
    assertEquals(List.of(
      new OsmReader.RelationMember<>("leafway", testRelInfo, List.of())
    ), feature1.relationInfo(TestRelInfo.class));
    assertEquals(List.of(
      new OsmReader.RelationMember<>("rolename", testRelInfo, List.of())
    ), feature2.relationInfo(TestRelInfo.class));
  }

  @Test
  void testSuperRelationOrderedEasily() {
    record TestRelInfo(long id, String name) implements OsmRelationInfo {}
    var testRelInfo = new TestRelInfo(1, "name");
    OsmReader reader = new OsmReader("osm", () -> osmSource, nodeMap, multipolygons, new Profile.NullProfile() {
      @Override
      public List<OsmRelationInfo> preprocessOsmRelation(OsmElement.Relation relation) {
        return List.of(testRelInfo);
      }
    }, stats);
    var nodeCache = reader.newNodeLocationProvider();
    long index = 1;
    var node1 = node(index++, 0, 0);
    var node2 = node(index++, 1, 1);
    var node3 = node(index++, 2, 2);
    var node4 = node(index++, 3, 3);
    var way1 = new OsmElement.Way(index++);
    way1.nodes().add(node1.id(), node2.id());
    way1.setTag("key", "value");
    var way2 = new OsmElement.Way(index++);
    way2.nodes().add(node3.id(), node4.id());
    way2.setTag("key", "value");
    var relation1 = new OsmElement.Relation(index++);
    var relation2 = new OsmElement.Relation(index++);
    var relation3 = new OsmElement.Relation(index);
    relation1.members().add(new OsmElement.Relation.Member(OsmElement.Type.WAY, way1.id(), "leafway"));

    relation2.members().add(new OsmElement.Relation.Member(OsmElement.Type.RELATION, relation1.id(), "midrel"));
    relation2.members().add(new OsmElement.Relation.Member(OsmElement.Type.WAY, way2.id(), "rolename"));

    relation3.members().add(new OsmElement.Relation.Member(OsmElement.Type.RELATION, relation1.id(), "topmosrel"));

    processPass1Block(reader, List.of(node1, node2, node3, node4, way1, way2, relation1, relation2, relation3));

    SourceFeature feature1 = reader.processWayPass2(way1, nodeCache);
    SourceFeature feature2 = reader.processWayPass2(way2, nodeCache);
    assertEquals(List.of(
      new OsmReader.RelationMember<>("leafway", testRelInfo, List.of()),
      new OsmReader.RelationMember<>("midrel", testRelInfo, List.of(relation1.id())),
      new OsmReader.RelationMember<>("topmosrel", testRelInfo, List.of(relation1.id()))
    ), feature1.relationInfo(TestRelInfo.class, true));
    assertEquals(List.of(
      new OsmReader.RelationMember<>("rolename", testRelInfo, List.of())
    ), feature2.relationInfo(TestRelInfo.class, true));
  }

  @Test
  void testSuperRelationOrderedBadly() {
    record TestRelInfo(long id, String name) implements OsmRelationInfo {}
    var testRelInfo = new TestRelInfo(1, "name");
    OsmReader reader = new OsmReader("osm", () -> osmSource, nodeMap, multipolygons, new Profile.NullProfile() {
      @Override
      public List<OsmRelationInfo> preprocessOsmRelation(OsmElement.Relation relation) {
        return List.of(testRelInfo);
      }
    }, stats);
    var nodeCache = reader.newNodeLocationProvider();
    long index = 1;
    var node1 = node(index++, 0, 0);
    var node2 = node(index++, 1, 1);
    var node3 = node(index++, 2, 2);
    var node4 = node(index++, 3, 3);
    var node5 = node(index++, 4, 4);
    var node6 = node(index++, 5, 5);
    var way1 = new OsmElement.Way(index++);
    way1.nodes().add(node1.id(), node2.id());
    way1.setTag("key", "value");
    var way2 = new OsmElement.Way(index++);
    way2.nodes().add(node3.id(), node4.id());
    way2.setTag("key", "value");
    var way3 = new OsmElement.Way(index++);
    way3.nodes().add(node5.id(), node6.id());
    way3.setTag("key", "value");
    var relation1 = new OsmElement.Relation(index++);
    var relation2 = new OsmElement.Relation(index++);
    var relation3 = new OsmElement.Relation(index);
    // way1 -> rel1
    // way2 -> rel2 -> rel1
    // way3 -> rel3 -> rel2 -> rel1
    relation1.members().add(new OsmElement.Relation.Member(OsmElement.Type.RELATION, relation2.id(), "rel2"));
    relation1.members().add(new OsmElement.Relation.Member(OsmElement.Type.WAY, way1.id(), "way1"));

    relation2.members().add(new OsmElement.Relation.Member(OsmElement.Type.RELATION, relation3.id(), "rel3"));
    relation2.members().add(new OsmElement.Relation.Member(OsmElement.Type.WAY, way2.id(), "way2"));

    relation3.members().add(new OsmElement.Relation.Member(OsmElement.Type.WAY, way3.id(), "way3"));

    processPass1Block(reader,
      List.of(node1, node2, node3, node4, node5, node6, way1, way2, way3, relation1, relation2, relation3));

    SourceFeature feature1 = reader.processWayPass2(way1, nodeCache);
    SourceFeature feature2 = reader.processWayPass2(way2, nodeCache);
    SourceFeature feature3 = reader.processWayPass2(way3, nodeCache);
    assertEquals(List.of(
      new OsmReader.RelationMember<>("way1", testRelInfo, List.of())
    ), feature1.relationInfo(TestRelInfo.class, true));
    assertEquals(List.of(
      new OsmReader.RelationMember<>("way2", testRelInfo, List.of()),
      new OsmReader.RelationMember<>("rel2", testRelInfo, List.of(relation2.id()))
    ), feature2.relationInfo(TestRelInfo.class, true));
    assertEquals(List.of(
      new OsmReader.RelationMember<>("way3", testRelInfo, List.of()),
      new OsmReader.RelationMember<>("rel3", testRelInfo, List.of(relation3.id())),
      new OsmReader.RelationMember<>("rel2", testRelInfo, List.of(relation3.id(), relation2.id()))
    ), feature3.relationInfo(TestRelInfo.class, true));
  }

  @Test
  void testCyclicSuperRelation() {
    record TestRelInfo(long id, String name) implements OsmRelationInfo {}
    var testRelInfo = new TestRelInfo(1, "name");
    OsmReader reader = new OsmReader("osm", () -> osmSource, nodeMap, multipolygons, new Profile.NullProfile() {
      @Override
      public List<OsmRelationInfo> preprocessOsmRelation(OsmElement.Relation relation) {
        return List.of(new TestRelInfo(1, "name"));
      }
    }, stats);
    var nodeCache = reader.newNodeLocationProvider();
    long index = 1;
    var node1 = node(index++, 0, 0);
    var node2 = node(index++, 1, 1);
    var way1 = new OsmElement.Way(index++);
    var relation1 = new OsmElement.Relation(index++);
    var relation2 = new OsmElement.Relation(index);
    relation1.members().add(new OsmElement.Relation.Member(OsmElement.Type.RELATION, relation2.id(), "rolename1"));
    relation1.members().add(new OsmElement.Relation.Member(OsmElement.Type.WAY, way1.id(), "rolename2"));

    relation2.members().add(new OsmElement.Relation.Member(OsmElement.Type.RELATION, relation1.id(), "rolename3"));

    processPass1Block(reader, List.of(node1, node2, way1, relation1, relation2));

    SourceFeature feature1 = reader.processWayPass2(way1, nodeCache);
    assertEquals(List.of(
      new OsmReader.RelationMember<>("rolename2", testRelInfo, List.of()),
      new OsmReader.RelationMember<>("rolename3", testRelInfo, List.of(relation1.id()))
    ), feature1.relationInfo(TestRelInfo.class, true));
  }

  private OsmReader newOsmReader() {
    return new OsmReader("osm", () -> osmSource, nodeMap, multipolygons, profile, stats);
  }

  @Test
  void testSplitLine() throws GeometryException {
    OsmReader reader = new OsmReader("osm", () -> osmSource, nodeMap, multipolygons, new Profile.NullProfile() {
      @Override
      public boolean splitOsmWayAtIntersections(OsmElement.Way way) {
        return true;
      }
    }, stats);
    var nodeCache = reader.newNodeLocationProvider();
    var node1 = node(1, 0, 0);
    var node2 = node(2, 0.5, 0.5);
    var node3 = node(3, 0.75, 0.75);
    var node4 = node(4, 0.75, 0.5);
    var way1 = new OsmElement.Way(1);
    var way2 = new OsmElement.Way(2);
    way1.nodes().add(node1.id(), node2.id(), node3.id());
    way2.nodes().add(node2.id(), node4.id());

    processPass1Block(reader, List.of(node1, node2, node3, node4, way1, way2));

    List<OsmReader.WaySourceFeature> features =
      reader.splitWayIfNecessary(way1, reader.processWayPass2(way1, nodeCache), 100);
    assertEquals(3, features.size());

    var f1 = features.getFirst();
    var f2 = features.get(1);
    var f3 = features.get(2);

    assertInstanceOf(FullWay.class, f1);
    assertTrue(f1.canBeLine());
    assertFalse(f1.isPoint());
    assertFalse(f1.canBePolygon());
    assertSameNormalizedFeature(
      newLineString(
        0, 0, 0.5, 0.5, 0.75, 0.75
      ),
      TestUtils.round(f1.worldGeometry()),
      TestUtils.round(f1.line()),
      TestUtils.round(GeoUtils.latLonToWorldCoords(f1.latLonGeometry()))
    );

    assertInstanceOf(SplitWay.class, f2);
    assertTrue(f2.canBeLine());
    assertFalse(f2.isPoint());
    assertFalse(f2.canBePolygon());
    assertSameNormalizedFeature(
      newLineString(
        0, 0, 0.5, 0.5
      ),
      TestUtils.round(f2.worldGeometry()),
      TestUtils.round(f2.line()),
      TestUtils.round(GeoUtils.latLonToWorldCoords(f2.latLonGeometry()))
    );
    assertEquals(1, ((SplitWay) f2).uniqueId());

    assertInstanceOf(SplitWay.class, f3);
    assertTrue(f3.canBeLine());
    assertFalse(f3.isPoint());
    assertFalse(f3.canBePolygon());
    assertSameNormalizedFeature(
      newLineString(
        0.5, 0.5, 0.75, 0.75
      ),
      TestUtils.round(f3.worldGeometry()),
      TestUtils.round(f3.line()),
      TestUtils.round(GeoUtils.latLonToWorldCoords(f3.latLonGeometry()))
    );
    assertEquals(101, ((SplitWay) f3).uniqueId());


    features =
      reader.splitWayIfNecessary(way2, reader.processWayPass2(way2, nodeCache), 100);
    assertEquals(1, features.size());

    var f4 = features.getFirst();

    assertFalse(f4 instanceof SplitWay, f4.getClass().getSimpleName());
    assertFalse(f4 instanceof FullWay, f4.getClass().getSimpleName());
    assertTrue(f4.canBeLine());
    assertFalse(f4.isPoint());
    assertFalse(f4.canBePolygon());
    assertSameNormalizedFeature(
      newLineString(
        0.5, 0.5, 0.75, 0.5
      ),
      TestUtils.round(f4.worldGeometry()),
      TestUtils.round(f4.line()),
      TestUtils.round(GeoUtils.latLonToWorldCoords(f4.latLonGeometry()))
    );
  }

  @Test
  void testSplitLineLoop() throws GeometryException {
    OsmReader reader = new OsmReader("osm", () -> osmSource, nodeMap, multipolygons, new Profile.NullProfile() {
      @Override
      public boolean splitOsmWayAtIntersections(OsmElement.Way way) {
        return true;
      }
    }, stats);
    var nodeCache = reader.newNodeLocationProvider();
    var node1 = node(1, 0, 0);
    var node2 = node(2, 0.5, 0.5);
    var node3 = node(3, 0.75, 0.75);
    var node4 = node(4, 0.75, 0.5);
    var way1 = new OsmElement.Way(1);
    var way2 = new OsmElement.Way(2);
    way1.nodes().add(node1.id(), node2.id(), node3.id(), node1.id());
    way2.nodes().add(node2.id(), node4.id());

    processPass1Block(reader, List.of(node1, node2, node3, node4, way1, way2));

    List<OsmReader.WaySourceFeature> features =
      reader.splitWayIfNecessary(way1, reader.processWayPass2(way1, nodeCache), 100);
    assertEquals(3, features.size());

    var f = features.getFirst();

    assertInstanceOf(FullWay.class, f);
    assertTrue(f.canBeLine());
    assertFalse(f.isPoint());
    assertTrue(f.canBePolygon());
    assertSameNormalizedFeature(
      newPolygon(
        0, 0, 0.5, 0.5, 0.75, 0.75, 0, 0
      ),
      TestUtils.round(f.worldGeometry()),
      TestUtils.round(f.polygon()),
      TestUtils.round(GeoUtils.latLonToWorldCoords(f.latLonGeometry()))
    );
    assertSameNormalizedFeature(
      newLineString(
        0, 0, 0.5, 0.5, 0.75, 0.75, 0, 0
      ),
      TestUtils.round(f.line())
    );

    f = features.get(1);

    assertInstanceOf(SplitWay.class, f);
    assertTrue(f.canBeLine());
    assertFalse(f.isPoint());
    assertFalse(f.canBePolygon());
    assertSameNormalizedFeature(
      newLineString(
        0, 0, 0.5, 0.5
      ),
      TestUtils.round(f.worldGeometry()),
      TestUtils.round(f.line()),
      TestUtils.round(GeoUtils.latLonToWorldCoords(f.latLonGeometry()))
    );

    f = features.get(2);

    assertInstanceOf(SplitWay.class, f);
    assertTrue(f.canBeLine());
    assertFalse(f.isPoint());
    assertFalse(f.canBePolygon());
    assertSameNormalizedFeature(
      newLineString(
        0.5, 0.5, 0.75, 0.75, 0, 0
      ),
      TestUtils.round(f.worldGeometry()),
      TestUtils.round(f.line()),
      TestUtils.round(GeoUtils.latLonToWorldCoords(f.latLonGeometry()))
    );
  }

  @Test
  void testDontSplitLineWithAreaYes() {
    OsmReader reader = new OsmReader("osm", () -> osmSource, nodeMap, multipolygons, new Profile.NullProfile() {
      @Override
      public boolean splitOsmWayAtIntersections(OsmElement.Way way) {
        return true;
      }
    }, stats);
    var nodeCache = reader.newNodeLocationProvider();
    var node1 = node(1, 0, 0);
    var node2 = node(2, 0.5, 0.5);
    var node3 = node(3, 0.75, 0.75);
    var node4 = node(4, 0.75, 0.5);
    var way1 = new OsmElement.Way(1, Map.of("area", "yes"), LongArrayList.from(1, 2, 3, 1));
    var way2 = new OsmElement.Way(2, Map.of(), LongArrayList.from(2, 4));

    processPass1Block(reader, List.of(node1, node2, node3, node4, way1, way2));

    List<OsmReader.WaySourceFeature> features =
      reader.splitWayIfNecessary(way1, reader.processWayPass2(way1, nodeCache), 100);
    assertEquals(1, features.size());

    var f = features.getFirst();

    assertFalse(f instanceof SplitWay, f.getClass().getSimpleName());
    assertFalse(f instanceof FullWay, f.getClass().getSimpleName());
  }
}
