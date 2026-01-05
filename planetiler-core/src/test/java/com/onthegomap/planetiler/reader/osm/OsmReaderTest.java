package com.onthegomap.planetiler.reader.osm;

import static com.onthegomap.planetiler.TestUtils.*;
import static org.junit.jupiter.api.Assertions.*;

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
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.LoggerConfig;
import java.util.ArrayList;
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

  /**
   * Custom appender to capture log events for testing.
   */
  private static class TestAppender extends AbstractAppender implements AutoCloseable {
    private final List<LogEvent> logEvents = new ArrayList<>();

    protected TestAppender(String name) {
      super(name, null, null, false, null);
    }

    @Override
    public void append(LogEvent event) {
      logEvents.add(event.toImmutable());
    }

    public List<LogEvent> getLogEvents() {
      return new ArrayList<>(logEvents);
    }

    @Override
    public void close() {
      stop();
    }
  }

  @Test
  void testIncompleteRelationLoggedAsWarningWithoutStackTrace() {
    // Set up TestAppender to capture log events
    LoggerContext context = (LoggerContext) LogManager.getContext(false);
    Configuration config = context.getConfiguration();
    LoggerConfig loggerConfig = config.getLoggerConfig("com.onthegomap.planetiler.reader.osm.OsmReader");
    TestAppender testAppender = new TestAppender("TestAppender");
    testAppender.start();
    loggerConfig.addAppender(testAppender, Level.ALL, null);
    context.updateLoggers();

    try (testAppender) {
      // Create a profile that throws an incomplete relation exception during preprocessing
      Profile testProfile = new Profile.NullProfile() {
        @Override
        public List<OsmRelationInfo> preprocessOsmRelation(OsmElement.Relation relation) {
          // Throw an exception that matches incomplete relation pattern
          throw new IllegalArgumentException("Missing location for node: 123");
        }
      };

      OsmReader reader = new OsmReader("osm", () -> osmSource, nodeMap, multipolygons, testProfile, stats);
      var relation = new OsmElement.Relation(6);
      relation.setTag("type", "multipolygon");

      // Process the relation - this should trigger the exception and log it as a warning
      processPass1Block(reader, List.of(relation));

      // Verify that incomplete relation exceptions are logged as WARN without stack trace
      List<LogEvent> events = testAppender.getLogEvents();
      boolean foundIncompleteWarning = false;
      for (LogEvent event : events) {
        String message = event.getMessage().getFormattedMessage();
        if (message.contains("Incomplete OSM relation")) {
          // Should be WARN level, not ERROR
          assertEquals(Level.WARN, event.getLevel(),
            "Incomplete relation exception should be logged as WARN, not " + event.getLevel());
          // Should not have a stack trace (throwable should be null)
          assertNull(event.getThrown(),
            "Incomplete relation exception should not include stack trace, but got: " + event.getThrown());
          foundIncompleteWarning = true;
        }
      }
      assertTrue(foundIncompleteWarning, "Should have logged a warning for incomplete relation. Events: " +
        events.stream().map(e -> e.getLevel() + ": " + e.getMessage().getFormattedMessage()).toList());
    } finally {
      // Clean up
      loggerConfig.removeAppender("TestAppender");
      context.updateLoggers();
    }
  }

  @Test
  void testIncompleteRelationExceptionDetection() {
    // Test various incomplete relation exception patterns
    assertTrue(isIncompleteRelationException(new IllegalArgumentException("Missing location for node: 123")));
    assertTrue(isIncompleteRelationException(new RuntimeException("error building multipolygon 123")));
    assertTrue(isIncompleteRelationException(new Exception("no rings to process")));
    assertTrue(isIncompleteRelationException(new Exception("multipolygon not closed")));
    assertTrue(isIncompleteRelationException(new Exception("missing_way")));
    assertTrue(isIncompleteRelationException(new Exception("missing node")));
    assertTrue(isIncompleteRelationException(
      new GeometryException("osm_invalid_multipolygon", "test")));
    assertTrue(isIncompleteRelationException(
      new GeometryException("osm_missing_way", "test")));
    assertTrue(isIncompleteRelationException(
      new RuntimeException("test", new IllegalArgumentException("Missing location for node: 123"))));

    // Test non-incomplete exceptions
    assertFalse(isIncompleteRelationException(new RuntimeException("Some other error")));
    assertFalse(isIncompleteRelationException(new GeometryException("other_error", "test")));
    assertFalse(isIncompleteRelationException(null));
    assertFalse(isIncompleteRelationException(new Exception())); // null message
  }

  @Test
  void testNonIncompleteRelationExceptionLoggedAsError() {
    // Set up TestAppender to capture log events
    LoggerContext context = (LoggerContext) LogManager.getContext(false);
    Configuration config = context.getConfiguration();
    LoggerConfig loggerConfig = config.getLoggerConfig("com.onthegomap.planetiler.reader.osm.OsmReader");
    TestAppender testAppender = new TestAppender("TestAppender");
    testAppender.start();
    loggerConfig.addAppender(testAppender, Level.ALL, null);
    context.updateLoggers();

    try (testAppender) {
      // Create a profile that throws a non-incomplete exception
      Profile testProfile = new Profile.NullProfile() {
        @Override
        public List<OsmRelationInfo> preprocessOsmRelation(OsmElement.Relation relation) {
          throw new RuntimeException("Unexpected error");
        }
      };

      OsmReader reader = new OsmReader("osm", () -> osmSource, nodeMap, multipolygons, testProfile, stats);
      var relation = new OsmElement.Relation(6);
      relation.setTag("type", "multipolygon");

      // Process the relation - should log as ERROR with stack trace
      processPass1Block(reader, List.of(relation));

      // Verify that non-incomplete exceptions are logged as ERROR
      List<LogEvent> events = testAppender.getLogEvents();
      boolean foundError = false;
      for (LogEvent event : events) {
        String message = event.getMessage().getFormattedMessage();
        if (message.contains("Error preprocessing OSM relation")) {
          assertEquals(Level.ERROR, event.getLevel(),
            "Non-incomplete exception should be logged as ERROR");
          assertNotNull(event.getThrown(),
            "Non-incomplete exception should include stack trace");
          foundError = true;
        }
      }
      assertTrue(foundError, "Should have logged an error for non-incomplete exception");
    } finally {
      // Clean up
      loggerConfig.removeAppender("TestAppender");
      context.updateLoggers();
    }
  }

  @Test
  void testIncompleteRelationExceptionWithVariousMessages() {
    // Test all the different message patterns that indicate incomplete relations
    assertTrue(isIncompleteRelationException(new Exception("error building multipolygon 123")));
    assertTrue(isIncompleteRelationException(new Exception("no rings to process")));
    assertTrue(isIncompleteRelationException(new Exception("multipolygon not closed")));
    assertTrue(isIncompleteRelationException(new Exception("missing_way")));
    assertTrue(isIncompleteRelationException(new Exception("missing node")));
  }

  @Test
  void testIncompleteRelationExceptionWithGeometryExceptionStats() {
    // Test GeometryException with different stat values
    assertTrue(isIncompleteRelationException(
      new GeometryException("osm_invalid_multipolygon", "test")));
    assertTrue(isIncompleteRelationException(
      new GeometryException("osm_missing_way", "test")));
    assertTrue(isIncompleteRelationException(
      new GeometryException("osm_missing_node", "test")));
    // Note: "other_invalid_multipolygon" contains "invalid_multipolygon" so it matches
    assertTrue(isIncompleteRelationException(
      new GeometryException("other_invalid_multipolygon", "test")));
    assertFalse(isIncompleteRelationException(
      new GeometryException("osm_other_error", "test")));
  }

  @Test
  void testIncompleteRelationExceptionWithCauseChain() {
    // Test exception with cause chain
    assertTrue(isIncompleteRelationException(
      new RuntimeException("outer", new IllegalArgumentException("Missing location for node: 123"))));
    assertTrue(isIncompleteRelationException(
      new RuntimeException("outer", new Exception("error building multipolygon"))));
    assertFalse(isIncompleteRelationException(
      new RuntimeException("outer", new Exception("other error"))));
  }

  @Test
  void testIncompleteRelationExceptionEdgeCases() {
    // Test edge cases
    assertFalse(isIncompleteRelationException(null));
    assertFalse(isIncompleteRelationException(new Exception())); // null message
    assertFalse(isIncompleteRelationException(new Exception(""))); // empty message
    assertFalse(isIncompleteRelationException(new RuntimeException("Some other error")));
  }


  // Helper method to access private method for testing
  private boolean isIncompleteRelationException(Throwable e) {
    // Use reflection to test the private method
    try {
      var method = OsmReader.class.getDeclaredMethod("isIncompleteRelationException", Throwable.class);
      method.setAccessible(true);
      return (Boolean) method.invoke(null, e);
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
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
}
