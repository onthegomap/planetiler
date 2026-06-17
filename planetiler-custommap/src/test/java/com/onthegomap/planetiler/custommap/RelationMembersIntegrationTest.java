package com.onthegomap.planetiler.custommap;

import static java.util.Collections.emptyList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.carrotsearch.hppc.LongArrayList;
import com.onthegomap.planetiler.FeatureCollector;
import com.onthegomap.planetiler.config.PlanetilerConfig;
import com.onthegomap.planetiler.custommap.configschema.SchemaConfig;
import com.onthegomap.planetiler.reader.SimpleFeature;
import com.onthegomap.planetiler.reader.SourceFeature;
import com.onthegomap.planetiler.reader.osm.OsmElement;
import com.onthegomap.planetiler.reader.osm.RelationMemberDataProvider;
import com.onthegomap.planetiler.reader.osm.RelationSourceFeature;
import com.onthegomap.planetiler.stats.Stats;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.locationtech.jts.geom.Coordinate;

/**
 * Integration tests for relation_members geometry type.
 * Tests the end-to-end flow of processing OSM relation members as separate features.
 */
class RelationMembersIntegrationTest {

  private PlanetilerConfig planetilerConfig = PlanetilerConfig.defaults();

  @BeforeEach
  void setUp() {
    planetilerConfig = PlanetilerConfig.defaults();
  }

  private ConfiguredProfile loadConfig(String config) {
    var schema = SchemaConfig.load(config);
    var root = Contexts.buildRootContext(planetilerConfig.arguments(), schema.args());
    planetilerConfig = root.config();
    return new ConfiguredProfile(schema, root);
  }

  private void testFeature(SourceFeature sf, Consumer<FeatureCollector.Feature> test, int expectedMatchCount,
    ConfiguredProfile profile) {
    var factory = new FeatureCollector.Factory(planetilerConfig, Stats.inMemory());
    var fc = factory.get(sf);

    profile.processFeature(sf, fc);

    var length = new AtomicInteger(0);

    fc.forEach(f -> {
      test.accept(f);
      length.incrementAndGet();
    });

    assertEquals(expectedMatchCount, length.get(), "Wrong number of features generated");
  }

  /**
   * Creates a mock RelationMemberDataProvider for testing.
   */
  private RelationMemberDataProvider createMockDataProvider(
    Map<Long, LongArrayList> wayGeometries,
    Map<Long, Map<String, Object>> wayTags,
    Map<Long, Map<String, Object>> nodeTags,
    Map<Long, Coordinate> nodeCoordinates) {
    return new RelationMemberDataProvider() {
      @Override
      public LongArrayList getWayGeometry(long wayId) {
        return wayGeometries.get(wayId);
      }

      @Override
      public Map<String, Object> getWayTags(long wayId) {
        return wayTags.get(wayId);
      }

      @Override
      public Map<String, Object> getNodeTags(long nodeId) {
        return nodeTags.get(nodeId);
      }

      @Override
      public Coordinate getNodeCoordinate(long nodeId) {
        return nodeCoordinates.get(nodeId);
      }
    };
  }

  /**
   * Test 1: Basic railway route with way members (GitHub Issue Example 1)
   */
  @Test
  void testRouteRelation_RailwaySegments() {
    var config = """
      sources:
        osm:
          type: osm
          url: geofabrik:rhode-island
      layers:
        - id: route_segments
          features:
            - source: osm
              geometry: relation_members
              include_when:
                type: route
                route: railway
              member_types: [way]
              member_include_when:
                railway: rail
              attributes:
                - key: route_name
                  tag_value: name
              member_attributes:
                - key: segment_ref
                  tag_value: ref
      """;

    // Create relation with 3 way members
    var relation = new OsmElement.Relation(100L, Map.of(
      "type", "route",
      "route", "railway",
      "name", "Main Line"
    ), List.of(
      new OsmElement.Relation.Member(OsmElement.Type.WAY, 1L, ""),
      new OsmElement.Relation.Member(OsmElement.Type.WAY, 2L, ""),
      new OsmElement.Relation.Member(OsmElement.Type.WAY, 3L, "")
    ));

    // Create mock data provider
    var wayGeometries = Map.of(
      1L, LongArrayList.from(10L, 11L, 12L),
      2L, LongArrayList.from(12L, 13L, 14L),
      3L, LongArrayList.from(14L, 15L, 16L)
    );
    var wayTags = Map.<Long, Map<String, Object>>of(
      1L, Map.of("railway", "rail", "ref", "A1"),
      2L, Map.of("railway", "rail", "ref", "A2"),
      3L, Map.of("railway", "rail", "ref", "A3")
    );
    var nodeCoordinates = Map.of(
      10L, new Coordinate(0.0, 0.0),
      11L, new Coordinate(0.1, 0.0),
      12L, new Coordinate(0.2, 0.0),
      13L, new Coordinate(0.3, 0.0),
      14L, new Coordinate(0.4, 0.0),
      15L, new Coordinate(0.5, 0.0),
      16L, new Coordinate(0.6, 0.0)
    );

    var dataProvider = createMockDataProvider(wayGeometries, wayTags, Map.of(), nodeCoordinates);
    var relationFeature = new RelationSourceFeature(relation, emptyList(), dataProvider);

    var profile = loadConfig(config);
    testFeature(relationFeature, feature -> {
      assertNotNull(feature.getAttrsAtZoom(14).get("route_name"));
      assertEquals("Main Line", feature.getAttrsAtZoom(14).get("route_name"));
      assertTrue(feature.getAttrsAtZoom(14).containsKey("segment_ref"));
    }, 3, profile);
  }

  /**
   * Test 2: Boundary relation with way members and role filtering (GitHub Issue Example 2)
   */
  @Test
  void testBoundaryRelation_Segments() {
    var config = """
      sources:
        osm:
          type: osm
          url: geofabrik:rhode-island
      layers:
        - id: boundary_segments
          features:
            - source: osm
              geometry: relation_members
              include_when:
                type: boundary
                admin_level: "6"
              member_types: [way]
              member_roles: ["outer"]
              member_attributes:
                - key: boundary_type
                  tag_value: boundary
      """;

    var relation = new OsmElement.Relation(200L, Map.of(
      "type", "boundary",
      "admin_level", "6",
      "name", "County"
    ), List.of(
      new OsmElement.Relation.Member(OsmElement.Type.WAY, 10L, "outer"),
      new OsmElement.Relation.Member(OsmElement.Type.WAY, 11L, "outer"),
      new OsmElement.Relation.Member(OsmElement.Type.WAY, 12L, "inner") // Should be filtered out
    ));

    var wayGeometries = Map.of(
      10L, LongArrayList.from(20L, 21L),
      11L, LongArrayList.from(21L, 22L),
      12L, LongArrayList.from(22L, 23L)
    );
    var wayTags = Map.<Long, Map<String, Object>>of(
      10L, Map.of("boundary", "administrative"),
      11L, Map.of("boundary", "administrative"),
      12L, Map.of("boundary", "administrative")
    );
    var nodeCoordinates = Map.of(
      20L, new Coordinate(0.0, 0.0),
      21L, new Coordinate(0.1, 0.0),
      22L, new Coordinate(0.2, 0.0),
      23L, new Coordinate(0.3, 0.0)
    );

    var dataProvider = createMockDataProvider(wayGeometries, wayTags, Map.of(), nodeCoordinates);
    var relationFeature = new RelationSourceFeature(relation, emptyList(), dataProvider);

    var profile = loadConfig(config);
    // Should only process 2 members (outer role), not 3
    testFeature(relationFeature, feature -> {
      assertTrue(feature.getAttrsAtZoom(14).containsKey("boundary_type"));
    }, 2, profile);
  }

  /**
   * Test 3: Member type filtering - only process way members
   */
  @Test
  void testMemberTypeFiltering() {
    var config = """
      sources:
        osm:
          type: osm
          url: geofabrik:rhode-island
      layers:
        - id: test_layer
          features:
            - source: osm
              geometry: relation_members
              include_when:
                type: route
              member_types: [way]
      """;

    var relation = new OsmElement.Relation(300L, Map.of("type", "route"), List.of(
      new OsmElement.Relation.Member(OsmElement.Type.WAY, 1L, ""),
      new OsmElement.Relation.Member(OsmElement.Type.NODE, 100L, ""), // Should be filtered out
      new OsmElement.Relation.Member(OsmElement.Type.WAY, 2L, "")
    ));

    var wayGeometries = Map.of(
      1L, LongArrayList.from(10L, 11L),
      2L, LongArrayList.from(11L, 12L)
    );
    var nodeCoordinates = Map.of(
      10L, new Coordinate(0.0, 0.0),
      11L, new Coordinate(0.1, 0.0),
      12L, new Coordinate(0.2, 0.0),
      100L, new Coordinate(0.5, 0.5) // Node member - should be skipped
    );

    var dataProvider = createMockDataProvider(wayGeometries, Map.of(), Map.of(), nodeCoordinates);
    var relationFeature = new RelationSourceFeature(relation, emptyList(), dataProvider);

    var profile = loadConfig(config);
    // Should only process 2 way members, not the node member
    testFeature(relationFeature, feature -> {
      assertNotNull(feature);
    }, 2, profile);
  }

  /**
   * Test 4: Member tag filtering with member_include_when
   */
  @Test
  void testMemberTagFiltering_IncludeWhen() {
    var config = """
      sources:
        osm:
          type: osm
          url: geofabrik:rhode-island
      layers:
        - id: test_layer
          features:
            - source: osm
              geometry: relation_members
              include_when:
                type: route
              member_types: [way]
              member_include_when:
                railway: rail
      """;

    var relation = new OsmElement.Relation(400L, Map.of("type", "route"), List.of(
      new OsmElement.Relation.Member(OsmElement.Type.WAY, 1L, ""),
      new OsmElement.Relation.Member(OsmElement.Type.WAY, 2L, ""),
      new OsmElement.Relation.Member(OsmElement.Type.WAY, 3L, "")
    ));

    var wayGeometries = Map.of(
      1L, LongArrayList.from(10L, 11L),
      2L, LongArrayList.from(11L, 12L),
      3L, LongArrayList.from(12L, 13L)
    );
    var wayTags = Map.<Long, Map<String, Object>>of(
      1L, Map.of("railway", "rail"), // Should match
      2L, Map.of("railway", "rail"), // Should match
      3L, Map.of("highway", "primary") // Should be filtered out
    );
    var nodeCoordinates = Map.of(
      10L, new Coordinate(0.0, 0.0),
      11L, new Coordinate(0.1, 0.0),
      12L, new Coordinate(0.2, 0.0),
      13L, new Coordinate(0.3, 0.0)
    );

    var dataProvider = createMockDataProvider(wayGeometries, wayTags, Map.of(), nodeCoordinates);
    var relationFeature = new RelationSourceFeature(relation, emptyList(), dataProvider);

    var profile = loadConfig(config);
    // Should only process 2 members with railway=rail
    testFeature(relationFeature, feature -> {
      assertNotNull(feature);
    }, 2, profile);
  }

  /**
   * Test 5: Member tag filtering with member_exclude_when
   */
  @Test
  void testMemberTagFiltering_ExcludeWhen() {
    var config = """
      sources:
        osm:
          type: osm
          url: geofabrik:rhode-island
      layers:
        - id: test_layer
          features:
            - source: osm
              geometry: relation_members
              include_when:
                type: route
              member_types: [way]
              member_exclude_when:
                service: __any__
      """;

    var relation = new OsmElement.Relation(500L, Map.of("type", "route"), List.of(
      new OsmElement.Relation.Member(OsmElement.Type.WAY, 1L, ""),
      new OsmElement.Relation.Member(OsmElement.Type.WAY, 2L, ""),
      new OsmElement.Relation.Member(OsmElement.Type.WAY, 3L, "")
    ));

    var wayGeometries = Map.of(
      1L, LongArrayList.from(10L, 11L),
      2L, LongArrayList.from(11L, 12L),
      3L, LongArrayList.from(12L, 13L)
    );
    var wayTags = Map.<Long, Map<String, Object>>of(
      1L, Map.of("highway", "primary"), // Should match
      2L, Map.of("highway", "primary", "service", "yes"), // Should be excluded
      3L, Map.of("highway", "primary") // Should match
    );
    var nodeCoordinates = Map.of(
      10L, new Coordinate(0.0, 0.0),
      11L, new Coordinate(0.1, 0.0),
      12L, new Coordinate(0.2, 0.0),
      13L, new Coordinate(0.3, 0.0)
    );

    var dataProvider = createMockDataProvider(wayGeometries, wayTags, Map.of(), nodeCoordinates);
    var relationFeature = new RelationSourceFeature(relation, emptyList(), dataProvider);

    var profile = loadConfig(config);
    // Should only process 2 members (excluding the one with service tag)
    testFeature(relationFeature, feature -> {
      assertNotNull(feature);
    }, 2, profile);
  }

  /**
   * Test 6: Member tag filtering with member_include_when using inline script
   */
  @Test
  void testMemberTagFiltering_IncludeWhen_InlineScript() {
    var config = """
      sources:
        osm:
          type: osm
          url: geofabrik:rhode-island
      layers:
        - id: test_layer
          features:
            - source: osm
              geometry: relation_members
              include_when:
                type: route
              member_types: [way]
              member_include_when: '${ member.tags.railway == "rail" && member.tags.electrified != null }'
      """;

    var relation = new OsmElement.Relation(600L, Map.of("type", "route"), List.of(
      new OsmElement.Relation.Member(OsmElement.Type.WAY, 1L, ""),
      new OsmElement.Relation.Member(OsmElement.Type.WAY, 2L, ""),
      new OsmElement.Relation.Member(OsmElement.Type.WAY, 3L, "")
    ));

    var wayGeometries = Map.of(
      1L, LongArrayList.from(10L, 11L),
      2L, LongArrayList.from(11L, 12L),
      3L, LongArrayList.from(12L, 13L)
    );
    var wayTags = Map.<Long, Map<String, Object>>of(
      1L, Map.of("railway", "rail", "electrified", "contact_line"), // Should match
      2L, Map.of("railway", "rail"), // Should be filtered out (no electrified tag)
      3L, Map.of("highway", "primary") // Should be filtered out
    );
    var nodeCoordinates = Map.of(
      10L, new Coordinate(0.0, 0.0),
      11L, new Coordinate(0.1, 0.0),
      12L, new Coordinate(0.2, 0.0),
      13L, new Coordinate(0.3, 0.0)
    );

    var dataProvider = createMockDataProvider(wayGeometries, wayTags, Map.of(), nodeCoordinates);
    var relationFeature = new RelationSourceFeature(relation, emptyList(), dataProvider);

    var profile = loadConfig(config);
    // Should only process 1 member (railway=rail with electrified tag)
    testFeature(relationFeature, feature -> {
      assertNotNull(feature);
    }, 1, profile);
  }

  /**
   * Test 7: Member tag filtering with member_exclude_when using inline script
   */
  @Test
  void testMemberTagFiltering_ExcludeWhen_InlineScript() {
    var config = """
      sources:
        osm:
          type: osm
          url: geofabrik:rhode-island
      layers:
        - id: test_layer
          features:
            - source: osm
              geometry: relation_members
              include_when:
                type: route
              member_types: [way]
              member_exclude_when: '${ member.tags.service != null || member.tags.access == "private" }'
      """;

    var relation = new OsmElement.Relation(700L, Map.of("type", "route"), List.of(
      new OsmElement.Relation.Member(OsmElement.Type.WAY, 1L, ""),
      new OsmElement.Relation.Member(OsmElement.Type.WAY, 2L, ""),
      new OsmElement.Relation.Member(OsmElement.Type.WAY, 3L, ""),
      new OsmElement.Relation.Member(OsmElement.Type.WAY, 4L, "")
    ));

    var wayGeometries = Map.of(
      1L, LongArrayList.from(10L, 11L),
      2L, LongArrayList.from(11L, 12L),
      3L, LongArrayList.from(12L, 13L),
      4L, LongArrayList.from(13L, 14L)
    );
    var wayTags = Map.<Long, Map<String, Object>>of(
      1L, Map.of("highway", "primary"), // Should match
      2L, Map.of("highway", "primary", "service", "yes"), // Should be excluded (has service)
      3L, Map.of("highway", "primary", "access", "private"), // Should be excluded (access=private)
      4L, Map.of("highway", "primary") // Should match
    );
    var nodeCoordinates = Map.of(
      10L, new Coordinate(0.0, 0.0),
      11L, new Coordinate(0.1, 0.0),
      12L, new Coordinate(0.2, 0.0),
      13L, new Coordinate(0.3, 0.0),
      14L, new Coordinate(0.4, 0.0)
    );

    var dataProvider = createMockDataProvider(wayGeometries, wayTags, Map.of(), nodeCoordinates);
    var relationFeature = new RelationSourceFeature(relation, emptyList(), dataProvider);

    var profile = loadConfig(config);
    // Should only process 2 members (excluding those with service tag or access=private)
    testFeature(relationFeature, feature -> {
      assertNotNull(feature);
    }, 2, profile);
  }

  /**
   * Test 8: Node member processing (bus stops example)
   */
  @Test
  void testNodeMember_Processing() {
    var config = """
      sources:
        osm:
          type: osm
          url: geofabrik:rhode-island
      layers:
        - id: bus_stops
          features:
            - source: osm
              geometry: relation_members
              include_when:
                type: route
                route: bus
              member_types: [node]
              member_roles: ["stop"]
              member_include_when:
                public_transport: stop_position
              attributes:
                - key: route_ref
                  tag_value: ref
              member_attributes:
                - key: stop_name
                  tag_value: name
      """;

    var relation = new OsmElement.Relation(600L, Map.of(
      "type", "route",
      "route", "bus",
      "ref", "42"
    ), List.of(
      new OsmElement.Relation.Member(OsmElement.Type.NODE, 100L, "stop"),
      new OsmElement.Relation.Member(OsmElement.Type.NODE, 101L, "stop"),
      new OsmElement.Relation.Member(OsmElement.Type.NODE, 102L, "platform") // Wrong role
    ));

    var nodeTags = Map.<Long, Map<String, Object>>of(
      100L, Map.of("public_transport", "stop_position", "name", "Stop A"),
      101L, Map.of("public_transport", "stop_position", "name", "Stop B"),
      102L, Map.of("public_transport", "stop_position", "name", "Stop C")
    );
    var nodeCoordinates = Map.of(
      100L, new Coordinate(0.0, 0.0),
      101L, new Coordinate(0.1, 0.0),
      102L, new Coordinate(0.2, 0.0)
    );

    var dataProvider = createMockDataProvider(Map.of(), Map.of(), nodeTags, nodeCoordinates);
    var relationFeature = new RelationSourceFeature(relation, emptyList(), dataProvider);

    var profile = loadConfig(config);
    // Should process 2 node members with role "stop" and public_transport tag
    testFeature(relationFeature, feature -> {
      assertEquals("42", feature.getAttrsAtZoom(14).get("route_ref"));
      assertTrue(feature.getAttrsAtZoom(14).containsKey("stop_name"));
    }, 2, profile);
  }

  /**
   * Test 9: Duplicate member handling
   */
  @Test
  void testDuplicateMembers() {
    var config = """
      sources:
        osm:
          type: osm
          url: geofabrik:rhode-island
      layers:
        - id: test_layer
          features:
            - source: osm
              geometry: relation_members
              include_when:
                type: route
              member_types: [way]
      """;

    // Relation with same member referenced twice
    var relation = new OsmElement.Relation(700L, Map.of("type", "route"), List.of(
      new OsmElement.Relation.Member(OsmElement.Type.WAY, 1L, ""),
      new OsmElement.Relation.Member(OsmElement.Type.WAY, 1L, ""), // Duplicate
      new OsmElement.Relation.Member(OsmElement.Type.WAY, 2L, "")
    ));

    var wayGeometries = Map.of(
      1L, LongArrayList.from(10L, 11L),
      2L, LongArrayList.from(11L, 12L)
    );
    var nodeCoordinates = Map.of(
      10L, new Coordinate(0.0, 0.0),
      11L, new Coordinate(0.1, 0.0),
      12L, new Coordinate(0.2, 0.0)
    );

    var dataProvider = createMockDataProvider(wayGeometries, Map.of(), Map.of(), nodeCoordinates);
    var relationFeature = new RelationSourceFeature(relation, emptyList(), dataProvider);

    var profile = loadConfig(config);
    // Should only process 2 unique members (duplicate skipped)
    testFeature(relationFeature, feature -> {
      assertNotNull(feature);
    }, 2, profile);
  }

  /**
   * Test 10: Way member as polygon (closed way with area tag)
   */
  @Test
  void testWayMember_Polygon() {
    var config = """
      sources:
        osm:
          type: osm
          url: geofabrik:rhode-island
      layers:
        - id: test_layer
          features:
            - source: osm
              geometry: relation_members
              include_when:
                type: boundary
              member_types: [way]
      """;

    var relation = new OsmElement.Relation(800L, Map.of("type", "boundary"), List.of(
      new OsmElement.Relation.Member(OsmElement.Type.WAY, 1L, "outer")
    ));

    // Closed way (first and last node are the same)
    var wayGeometries = Map.of(
      1L, LongArrayList.from(10L, 11L, 12L, 10L) // Closed way
    );
    var wayTags = Map.<Long, Map<String, Object>>of(
      1L, Map.of("area", "yes") // Area tag present
    );
    var nodeCoordinates = Map.of(
      10L, new Coordinate(0.0, 0.0),
      11L, new Coordinate(0.1, 0.0),
      12L, new Coordinate(0.1, 0.1)
    );

    var dataProvider = createMockDataProvider(wayGeometries, wayTags, Map.of(), nodeCoordinates);
    var relationFeature = new RelationSourceFeature(relation, emptyList(), dataProvider);

    var profile = loadConfig(config);
    // Should create polygon geometry
    testFeature(relationFeature, feature -> {
      assertNotNull(feature);
      assertTrue(feature.getGeometry() instanceof org.locationtech.jts.geom.Polygon);
    }, 1, profile);
  }

  /**
   * Test 11: Missing member geometry handling
   */
  @Test
  void testMissingMemberGeometry() {
    var config = """
      sources:
        osm:
          type: osm
          url: geofabrik:rhode-island
      layers:
        - id: test_layer
          features:
            - source: osm
              geometry: relation_members
              include_when:
                type: route
              member_types: [way]
      """;

    var relation = new OsmElement.Relation(900L, Map.of("type", "route"), List.of(
      new OsmElement.Relation.Member(OsmElement.Type.WAY, 1L, ""),
      new OsmElement.Relation.Member(OsmElement.Type.WAY, 2L, "") // Missing geometry
    ));

    // Only way 1 has geometry
    var wayGeometries = Map.of(
      1L, LongArrayList.from(10L, 11L)
    );
    var nodeCoordinates = Map.of(
      10L, new Coordinate(0.0, 0.0),
      11L, new Coordinate(0.1, 0.0)
    );

    var dataProvider = createMockDataProvider(wayGeometries, Map.of(), Map.of(), nodeCoordinates);
    var relationFeature = new RelationSourceFeature(relation, emptyList(), dataProvider);

    var profile = loadConfig(config);
    // Should only process way 1 (way 2 has missing geometry)
    testFeature(relationFeature, feature -> {
      assertNotNull(feature);
    }, 1, profile);
  }

  /**
   * Test 12: Member attributes with inline script expressions
   */
  @Test
  void testMemberAttributes_InlineScript() {
    var config = """
      sources:
        osm:
          type: osm
          url: geofabrik:rhode-island
      layers:
        - id: route_segments
          features:
            - source: osm
              geometry: relation_members
              include_when:
                type: route
              member_types: [way]
              attributes:
                - key: route_name
                  tag_value: name
              member_attributes:
                - key: segment_ref
                  tag_value: ref
                - key: combined_name
                  value: '${ member.tags.name + " (" + feature.tags.name + ")" }'
      """;

    var relation = new OsmElement.Relation(1000L, Map.of(
      "type", "route",
      "name", "Main Route"
    ), List.of(
      new OsmElement.Relation.Member(OsmElement.Type.WAY, 1L, "")
    ));

    var wayGeometries = Map.of(
      1L, LongArrayList.from(10L, 11L)
    );
    var wayTags = Map.<Long, Map<String, Object>>of(
      1L, Map.of("ref", "A1", "name", "Segment 1")
    );
    var nodeCoordinates = Map.of(
      10L, new Coordinate(0.0, 0.0),
      11L, new Coordinate(0.1, 0.0)
    );

    var dataProvider = createMockDataProvider(wayGeometries, wayTags, Map.of(), nodeCoordinates);
    var relationFeature = new RelationSourceFeature(relation, emptyList(), dataProvider);

    var profile = loadConfig(config);
    testFeature(relationFeature, feature -> {
      assertEquals("Main Route", feature.getAttrsAtZoom(14).get("route_name"));
      assertEquals("A1", feature.getAttrsAtZoom(14).get("segment_ref"));
      assertEquals("Segment 1 (Main Route)", feature.getAttrsAtZoom(14).get("combined_name"));
    }, 1, profile);
  }

  /**
   * Test 13: Same relation processed as both polygon (multipolygon) and relation_members
   * 
   * This test verifies that a relation with type=boundary can be processed as both:
   * 1. A multipolygon (geometry: polygon) - creates one polygon feature from the relation
   * 2. Relation members (geometry: relation_members) - creates individual features for each member
   */
  @Test
  void testBoundaryRelation_BothPolygonAndRelationMembers() {
    var config = """
      sources:
        osm:
          type: osm
          url: geofabrik:rhode-island
      layers:
        - id: boundary_polygon
          features:
            - source: osm
              geometry: polygon
              include_when:
                type: boundary
                admin_level: "6"
              attributes:
                - key: boundary_name
                  tag_value: name
        - id: boundary_segments
          features:
            - source: osm
              geometry: relation_members
              include_when:
                type: boundary
                admin_level: "6"
              member_types: [way]
              member_roles: ["outer"]
              attributes:
                - key: boundary_name
                  tag_value: name
              member_attributes:
                - key: segment_id
                  value: '${ member.ref }'
      """;

    // Create a boundary relation that matches both criteria
    var relation = new OsmElement.Relation(1100L, Map.of(
      "type", "boundary",
      "admin_level", "6",
      "name", "Test County"
    ), List.of(
      new OsmElement.Relation.Member(OsmElement.Type.WAY, 20L, "outer"),
      new OsmElement.Relation.Member(OsmElement.Type.WAY, 21L, "outer")
    ));

    // Create way geometries for relation_members processing
    var wayGeometries = Map.of(
      20L, LongArrayList.from(30L, 31L, 32L),
      21L, LongArrayList.from(32L, 33L, 34L)
    );
    var wayTags = Map.<Long, Map<String, Object>>of(
      20L, Map.of("boundary", "administrative"),
      21L, Map.of("boundary", "administrative")
    );
    var nodeCoordinates = Map.of(
      30L, new Coordinate(0.0, 0.0),
      31L, new Coordinate(0.1, 0.0),
      32L, new Coordinate(0.2, 0.0),
      33L, new Coordinate(0.3, 0.0),
      34L, new Coordinate(0.4, 0.0)
    );

    var dataProvider = createMockDataProvider(wayGeometries, wayTags, Map.of(), nodeCoordinates);
    var relationFeature = new RelationSourceFeature(relation, emptyList(), dataProvider);

    var profile = loadConfig(config);
    
    // Test 1: Process as relation_members - should generate 2 member features
    var factory = new FeatureCollector.Factory(planetilerConfig, Stats.inMemory());
    var fc = factory.get(relationFeature);
    profile.processFeature(relationFeature, fc);

    var relationMembersCount = new AtomicInteger(0);
    var polygonCount = new AtomicInteger(0);
    
    fc.forEach(f -> {
      if ("boundary_segments".equals(f.getLayer())) {
        relationMembersCount.incrementAndGet();
        // Verify relation-level attributes are present
        assertEquals("Test County", f.getAttrsAtZoom(14).get("boundary_name"));
        // Verify member-level attributes are present
        assertTrue(f.getAttrsAtZoom(14).containsKey("segment_id"));
      } else if ("boundary_polygon".equals(f.getLayer())) {
        polygonCount.incrementAndGet();
        assertEquals("Test County", f.getAttrsAtZoom(14).get("boundary_name"));
      }
    });

    // Should have 2 relation_members features (one for each way member)
    assertEquals(2, relationMembersCount.get(), 
      "Should generate 2 features from relation_members geometry");
    // Note: polygon feature would be generated from MultipolygonSourceFeature in OsmReader,
    // but we're testing at ConfiguredProfile level with RelationSourceFeature, so polygon
    // won't be generated here. The important part is that both feature definitions exist
    // and the relation_members one works correctly.
    assertEquals(0, polygonCount.get(),
      "Polygon features are generated from MultipolygonSourceFeature in OsmReader, not RelationSourceFeature");
    
    // Test 2: Verify that a polygon feature with the same tags would match the polygon definition
    // This demonstrates that the same relation (by tags) can match both definitions
    var polygonFeature = SimpleFeature.createFakeOsmFeature(
      com.onthegomap.planetiler.TestUtils.newPolygon(0, 0, 1, 0, 1, 1, 0, 0),
      Map.of("type", "boundary", "admin_level", "6", "name", "Test County"),
      "osm", null, 1100L, emptyList(), null
    );
    
    var fc2 = factory.get(polygonFeature);
    profile.processFeature(polygonFeature, fc2);
    
    var polygonMatchCount = new AtomicInteger(0);
    fc2.forEach(f -> {
      if ("boundary_polygon".equals(f.getLayer())) {
        polygonMatchCount.incrementAndGet();
        assertEquals("Test County", f.getAttrsAtZoom(14).get("boundary_name"));
      }
    });
    
    // Should match the polygon definition
    assertEquals(1, polygonMatchCount.get(),
      "Polygon feature should match the polygon geometry definition");
  }
}

