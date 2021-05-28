package com.onthegomap.flatmap.read;

import static com.onthegomap.flatmap.TestUtils.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.graphhopper.reader.ReaderNode;
import com.graphhopper.reader.ReaderRelation;
import com.graphhopper.reader.ReaderWay;
import com.onthegomap.flatmap.Profile;
import com.onthegomap.flatmap.SourceFeature;
import com.onthegomap.flatmap.TestUtils;
import com.onthegomap.flatmap.collections.LongLongMap;
import com.onthegomap.flatmap.geo.GeoUtils;
import com.onthegomap.flatmap.geo.GeometryException;
import com.onthegomap.flatmap.monitoring.Stats;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

public class OpenStreetMapReaderTest {

  public final OsmSource osmSource = threads -> next -> {
  };
  private final Stats stats = new Stats.InMemory();
  private final Profile profile = new Profile.NullProfile();
  private final LongLongMap longLongMap = LongLongMap.newInMemoryHashMap();

  private static Profile newProfile(Function<ReaderRelation, List<OpenStreetMapReader.RelationInfo>> processRelation) {
    return new Profile.NullProfile() {
      @Override
      public List<OpenStreetMapReader.RelationInfo> preprocessOsmRelation(ReaderRelation relation) {
        return processRelation.apply(relation);
      }
    };
  }

  @Test
  public void testPoint() throws GeometryException {
    OpenStreetMapReader reader = new OpenStreetMapReader(
      osmSource,
      longLongMap,
      profile,
      stats
    );
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
    assertEquals(Map.of("key", "value"), feature.properties());
  }

  @Test
  public void testLine() throws GeometryException {
    OpenStreetMapReader reader = new OpenStreetMapReader(
      osmSource,
      longLongMap,
      profile,
      stats
    );
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
    assertEquals(Map.of("key", "value"), feature.properties());
  }

  @Test
  public void testPolygonAreaNotSpecified() throws GeometryException {
    OpenStreetMapReader reader = new OpenStreetMapReader(
      osmSource,
      longLongMap,
      profile,
      stats
    );
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
    OpenStreetMapReader reader = new OpenStreetMapReader(
      osmSource,
      longLongMap,
      profile,
      stats
    );
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
    OpenStreetMapReader reader = new OpenStreetMapReader(
      osmSource,
      longLongMap,
      profile,
      stats
    );
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
    OpenStreetMapReader reader = new OpenStreetMapReader(
      osmSource,
      longLongMap,
      profile,
      stats
    );
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
    OpenStreetMapReader reader = new OpenStreetMapReader(
      osmSource,
      longLongMap,
      profile,
      stats
    );
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
    OpenStreetMapReader reader = new OpenStreetMapReader(
      osmSource,
      longLongMap,
      profile,
      stats
    );

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

  @NotNull
  private ReaderNode node(long id, double x, double y) {
    return new ReaderNode(id, GeoUtils.getWorldLat(y), GeoUtils.getWorldLon(x));
  }

  @Test
  @Disabled
  public void testLineReferencingNonexistentNode() {
    OpenStreetMapReader reader = new OpenStreetMapReader(
      osmSource,
      longLongMap,
      profile,
      stats
    );
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

  @Test
  @Disabled
  public void testMultiPolygon() {
  }

  @Test
  @Disabled
  public void testMultiPolygonInfersCorrectParents() {
  }

  @Test
  @Disabled
  public void testInvalidMultiPolygon() {
  }

  @Test
  @Disabled
  public void testMultiPolygonRefersToNonexistentWay() {
  }

  // TODO what about:
  // - relation info / storage size
  // - multilevel multipolygon relationship containers
}
