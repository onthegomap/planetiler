package com.onthegomap.flatmap.reader.osm;

import static com.onthegomap.flatmap.TestUtils.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.graphhopper.reader.ReaderElement;
import com.graphhopper.reader.ReaderNode;
import com.graphhopper.reader.ReaderRelation;
import com.graphhopper.reader.ReaderWay;
import com.onthegomap.flatmap.Profile;
import com.onthegomap.flatmap.TestUtils;
import com.onthegomap.flatmap.collection.LongLongMap;
import com.onthegomap.flatmap.geo.GeoUtils;
import com.onthegomap.flatmap.geo.GeometryException;
import com.onthegomap.flatmap.reader.SourceFeature;
import com.onthegomap.flatmap.stats.Stats;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;

public class OsmReaderTest {

  public final OsmSource osmSource = (name, threads) -> next -> {
  };
  private final Stats stats = Stats.inMemory();
  private final Profile profile = new Profile.NullProfile();
  private final LongLongMap longLongMap = LongLongMap.newInMemoryHashMap();

  private static Profile newProfile(
    Function<OsmElement.Relation, List<OsmReader.RelationInfo>> processRelation) {
    return new Profile.NullProfile() {
      @Override
      public List<OsmReader.RelationInfo> preprocessOsmRelation(OsmElement.Relation relation) {
        return processRelation.apply(relation);
      }
    };
  }

  @Test
  public void testPoint() throws GeometryException {
    OsmReader reader = newOsmReader();
    var node = new ReaderNode(1, 0, 0);
    node.setTag("key", "value");
    reader.processPass1(node);
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
    var nodeCache = reader.newNodeGeometryCache();
    var node1 = new ReaderNode(1, 0, 0);
    var node2 = node(2, 0.75, 0.75);
    var way = new ReaderWay(3);
    way.getNodes().add(node1.getId(), node2.getId());
    way.setTag("key", "value");

    reader.processPass1(node1);
    reader.processPass1(node2);
    reader.processPass1(way);

    SourceFeature feature = reader.processWayPass2(nodeCache, way);
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
    var nodeCache = reader.newNodeGeometryCache();
    var node1 = node(1, 0.5, 0.5);
    var node2 = node(2, 0.5, 0.75);
    var node3 = node(3, 0.75, 0.75);
    var node4 = node(4, 0.75, 0.5);
    var way = new ReaderWay(3);
    way.getNodes().add(1, 2, 3, 4, 1);
    way.setTag("key", "value");

    reader.processPass1(node1);
    reader.processPass1(node2);
    reader.processPass1(node3);
    reader.processPass1(node4);
    reader.processPass1(way);

    SourceFeature feature = reader.processWayPass2(nodeCache, way);
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
    var nodeCache = reader.newNodeGeometryCache();
    var node1 = node(1, 0.5, 0.5);
    var node2 = node(2, 0.5, 0.75);
    var node3 = node(3, 0.75, 0.75);
    var node4 = node(4, 0.75, 0.5);
    var way = new ReaderWay(3);
    way.getNodes().add(1, 2, 3, 4, 1);
    way.setTag("area", "yes");

    reader.processPass1(node1);
    reader.processPass1(node2);
    reader.processPass1(node3);
    reader.processPass1(node4);
    reader.processPass1(way);

    SourceFeature feature = reader.processWayPass2(nodeCache, way);
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
    var nodeCache = reader.newNodeGeometryCache();
    var node1 = node(1, 0.5, 0.5);
    var node2 = node(2, 0.5, 0.75);
    var node3 = node(3, 0.75, 0.75);
    var node4 = node(4, 0.75, 0.5);
    var way = new ReaderWay(5);
    way.getNodes().add(1, 2, 3, 4, 1);
    way.setTag("area", "no");

    reader.processPass1(node1);
    reader.processPass1(node2);
    reader.processPass1(node3);
    reader.processPass1(node4);
    reader.processPass1(way);

    SourceFeature feature = reader.processWayPass2(nodeCache, way);
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
    var way = new ReaderWay(3);
    way.getNodes().add(1);

    reader.processPass1(node1);
    reader.processPass1(way);

    SourceFeature feature = reader.processWayPass2(reader.newNodeGeometryCache(), way);
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
    var way = new ReaderWay(3);
    way.getNodes().add(1, 2, 1);

    reader.processPass1(node1);
    reader.processPass1(node2);
    reader.processPass1(way);

    SourceFeature feature = reader.processWayPass2(reader.newNodeGeometryCache(), way);
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

    reader.processPass1(node(1, 0.5, 0.5));
    reader.processPass1(node(2, 0.75, 0.5));
    reader.processPass1(node(3, 0.5, 0.75));
    reader.processPass1(node(4, 0.75, 0.75));
    var way = new ReaderWay(6);
    way.setTag("area", "yes");
    way.getNodes().add(1, 2, 3, 4, 1);
    reader.processPass1(way);

    SourceFeature feature = reader.processWayPass2(reader.newNodeGeometryCache(), way);
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

  private ReaderNode node(long id, double x, double y) {
    return new ReaderNode(id, GeoUtils.getWorldLat(y), GeoUtils.getWorldLon(x));
  }

  @Test
  public void testLineReferencingNonexistentNode() {
    OsmReader reader = newOsmReader();
    var way = new ReaderWay(321);
    way.getNodes().add(123, 2222, 333, 444, 123);
    reader.processPass1(way);

    SourceFeature feature = reader.processWayPass2(reader.newNodeGeometryCache(), way);
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

  private final Function<ReaderElement, Stream<ReaderNode>> nodes = elem ->
    elem instanceof ReaderNode node ? Stream.of(node) : Stream.empty();

  private final Function<ReaderElement, Stream<ReaderWay>> ways = elem ->
    elem instanceof ReaderWay way ? Stream.of(way) : Stream.empty();

  private final Function<ReaderElement, Stream<ReaderRelation>> rels = elem ->
    elem instanceof ReaderRelation rel ? Stream.of(rel) : Stream.empty();

  @Test
  public void testMultiPolygon() throws GeometryException {
    OsmReader reader = newOsmReader();
    var outerway = new ReaderWay(9);
    outerway.getNodes().add(1, 2, 3, 4, 1);
    var innerway = new ReaderWay(10);
    innerway.getNodes().add(5, 6, 7, 8, 5);

    var relation = new ReaderRelation(11);
    relation.setTag("type", "multipolygon");
    relation.add(new ReaderRelation.Member(ReaderRelation.WAY, outerway.getId(), "outer"));
    relation.add(new ReaderRelation.Member(ReaderRelation.WAY, innerway.getId(), "inner"));

    List<ReaderElement> elements = List.of(
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

    elements.forEach(reader::processPass1);
    elements.stream().flatMap(nodes).forEach(reader::processNodePass2);
    var nodeCache = reader.newNodeGeometryCache();
    elements.stream().flatMap(ways).forEach(way -> {
      reader.processWayPass2(nodeCache, way);
    });

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
    var outerway = new ReaderWay(13);
    outerway.getNodes().add(1, 2, 3, 4, 1);
    var innerway = new ReaderWay(14);
    innerway.getNodes().add(5, 6, 7, 8, 5);
    var innerinnerway = new ReaderWay(15);
    innerinnerway.getNodes().add(9, 10, 11, 12, 9);

    var relation = new ReaderRelation(16);
    relation.setTag("type", "multipolygon");
    relation.add(new ReaderRelation.Member(ReaderRelation.WAY, outerway.getId(), "outer"));
    relation.add(new ReaderRelation.Member(ReaderRelation.WAY, innerway.getId(), "inner"));
    // nested hole marked as inner, but should actually be outer
    relation.add(new ReaderRelation.Member(ReaderRelation.WAY, innerinnerway.getId(), "inner"));

    List<ReaderElement> elements = List.of(
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

    elements.forEach(reader::processPass1);
    elements.stream().flatMap(nodes).forEach(reader::processNodePass2);
    var nodeCache = reader.newNodeGeometryCache();
    elements.stream().flatMap(ways).forEach(way -> {
      reader.processWayPass2(nodeCache, way);
    });

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
    var outerway = new ReaderWay(13);
    outerway.getNodes().add(1, 2, 3, 4, 1);
    var innerway = new ReaderWay(14);
    innerway.getNodes().add(5, 6, 7, 8, 5);
    var innerinnerway = new ReaderWay(15);
    innerinnerway.getNodes().add(9, 10, 11, 12, 9);

    var relation = new ReaderRelation(16);
    relation.setTag("type", "multipolygon");
    relation.add(new ReaderRelation.Member(ReaderRelation.WAY, outerway.getId(), "outer"));
    relation.add(new ReaderRelation.Member(ReaderRelation.WAY, innerway.getId(), "inner"));
    // nested hole marked as inner, but should actually be outer
    relation.add(new ReaderRelation.Member(ReaderRelation.WAY, innerinnerway.getId(), "inner"));

    List<ReaderElement> elements = List.of(
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

    elements.forEach(reader::processPass1);
    elements.stream().flatMap(nodes).forEach(reader::processNodePass2);
    var nodeCache = reader.newNodeGeometryCache();
    elements.stream().flatMap(ways).forEach(way -> {
      reader.processWayPass2(nodeCache, way);
    });

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
    var outerway = new ReaderWay(5);
    outerway.getNodes().add(1, 2, 3, 4, 1);

    var relation = new ReaderRelation(6);
    relation.setTag("type", "multipolygon");
    relation.add(new ReaderRelation.Member(ReaderRelation.WAY, outerway.getId(), "outer"));

    List<ReaderElement> elements = List.of(
      node(1, 0.1, 0.1),
//      node(2, 0.9, 0.1), MISSING!
      node(3, 0.9, 0.9),
      node(4, 0.1, 0.9),

      outerway,

      relation
    );

    elements.forEach(reader::processPass1);
    elements.stream().flatMap(nodes).forEach(reader::processNodePass2);
    var nodeCache = reader.newNodeGeometryCache();
    elements.stream().flatMap(ways).forEach(way -> {
      reader.processWayPass2(nodeCache, way);
    });

    var feature = reader.processRelationPass2(relation, nodeCache);

    assertThrows(GeometryException.class, feature::worldGeometry);
    assertThrows(GeometryException.class, feature::polygon);
    assertThrows(GeometryException.class, feature::validatedPolygon);
  }

  @Test
  public void testMultiPolygonRefersToNonexistentWay() {
    OsmReader reader = newOsmReader();

    var relation = new ReaderRelation(6);
    relation.setTag("type", "multipolygon");
    relation.add(new ReaderRelation.Member(ReaderRelation.WAY, 5, "outer"));

    List<ReaderElement> elements = List.of(
      node(1, 0.1, 0.1),
      node(2, 0.9, 0.1),
      node(3, 0.9, 0.9),
      node(4, 0.1, 0.9),

//      outerway, // missing!

      relation
    );

    elements.forEach(reader::processPass1);
    elements.stream().flatMap(nodes).forEach(reader::processNodePass2);
    var nodeCache = reader.newNodeGeometryCache();
    elements.stream().flatMap(ways).forEach(way -> {
      reader.processWayPass2(nodeCache, way);
    });

    var feature = reader.processRelationPass2(relation, nodeCache);

    assertThrows(GeometryException.class, feature::worldGeometry);
    assertThrows(GeometryException.class, feature::polygon);
    assertThrows(GeometryException.class, feature::validatedPolygon);
  }

  @Test
  public void testWayInRelation() {
    record OtherRelInfo(long id) implements OsmReader.RelationInfo {}
    record TestRelInfo(long id, String name) implements OsmReader.RelationInfo {}
    OsmReader reader = new OsmReader(
      osmSource,
      longLongMap,
      new Profile.NullProfile() {
        @Override
        public List<OsmReader.RelationInfo> preprocessOsmRelation(OsmElement.Relation relation) {
          return List.of(new TestRelInfo(1, "name"));
        }
      },
      stats
    );
    var nodeCache = reader.newNodeGeometryCache();
    var node1 = new ReaderNode(1, 0, 0);
    var node2 = node(2, 0.75, 0.75);
    var way = new ReaderWay(3);
    way.getNodes().add(node1.getId(), node2.getId());
    way.setTag("key", "value");
    var relation = new ReaderRelation(4);
    relation.add(new ReaderRelation.Member(ReaderRelation.Member.WAY, 3, "rolename"));

    reader.processPass1(node1);
    reader.processPass1(node2);
    reader.processPass1(way);
    reader.processPass1(relation);

    SourceFeature feature = reader.processWayPass2(nodeCache, way);

    assertEquals(List.of(), feature.relationInfo(OtherRelInfo.class));
    assertEquals(List.of(new OsmReader.RelationMember<>("rolename", new TestRelInfo(1, "name"))),
      feature.relationInfo(TestRelInfo.class));
  }

  private OsmReader newOsmReader() {
    return new OsmReader(
      osmSource,
      longLongMap,
      profile,
      stats
    );
  }

  // TODO: relation info / storage size
}
