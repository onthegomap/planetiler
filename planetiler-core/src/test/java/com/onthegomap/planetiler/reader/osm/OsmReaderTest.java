package com.onthegomap.planetiler.reader.osm;

import static com.onthegomap.planetiler.TestUtils.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

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

public class OsmReaderTest {

  public final OsmBlockSource osmSource = next -> {
  };
  private final Stats stats = Stats.inMemory();
  private final Profile profile = new Profile.NullProfile();
  private final LongLongMap nodeMap = LongLongMap.newInMemorySortedTable();
  private final LongLongMultimap multipolygons = LongLongMultimap.newInMemoryDenseOrderedMultimap();

  private void processPass1Block(OsmReader reader, Iterable<OsmElement> block) {
    reader.processPass1Blocks(List.of(block));
  }

  @Test
  public void testPoint() throws GeometryException {
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
  public void testLine() throws GeometryException {
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
  public void testPolygonAreaNotSpecified() throws GeometryException {
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
  public void testPolygonAreaYes() throws GeometryException {
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
  public void testPolygonAreaNo() throws GeometryException {
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
  public void testLineWithTooFewPoints() throws GeometryException {
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
  public void testPolygonWithTooFewPoints() throws GeometryException {
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
  public void testInvalidPolygon() throws GeometryException {
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
  public void testLineReferencingNonexistentNode() {
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
  public void testMultiPolygon(String relationType) throws GeometryException {
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
  public void testMultipolygonInfersCorrectParent() throws GeometryException {
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
  public void testInvalidMultipolygon() throws GeometryException {
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
  public void testMultiPolygonRefersToNonexistentNode() {
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
  public void testMultiPolygonRefersToNonexistentWay() {
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
  public void testWayInRelation() {
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
    assertEquals(List.of(new OsmReader.RelationMember<>("rolename", new TestRelInfo(1, "name"))),
      feature.relationInfo(TestRelInfo.class));
  }

  @Test
  public void testNodeOrWayRelationInRelationDoesntTriggerWay() {
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

  private OsmReader newOsmReader() {
    return new OsmReader("osm", () -> osmSource, nodeMap, multipolygons, profile, stats);
  }
}
