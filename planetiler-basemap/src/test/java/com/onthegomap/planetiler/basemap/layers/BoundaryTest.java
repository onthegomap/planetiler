package com.onthegomap.planetiler.basemap.layers;

import static com.onthegomap.planetiler.TestUtils.newLineString;
import static com.onthegomap.planetiler.TestUtils.rectangle;
import static com.onthegomap.planetiler.basemap.BasemapProfile.NATURAL_EARTH_SOURCE;
import static com.onthegomap.planetiler.basemap.BasemapProfile.OSM_SOURCE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import com.onthegomap.planetiler.FeatureCollector;
import com.onthegomap.planetiler.geo.GeoUtils;
import com.onthegomap.planetiler.geo.GeometryException;
import com.onthegomap.planetiler.reader.SimpleFeature;
import com.onthegomap.planetiler.reader.osm.OsmElement;
import com.onthegomap.planetiler.reader.osm.OsmReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;

public class BoundaryTest extends AbstractLayerTest {

  @Test
  public void testNaturalEarthCountryBoundaries() {
    assertCoversZoomRange(
      0, 4, "boundary",
      process(SimpleFeature.create(
        newLineString(0, 0, 1, 1),
        Map.of(),
        NATURAL_EARTH_SOURCE,
        "ne_110m_admin_0_boundary_lines_land",
        0
      )),
      process(SimpleFeature.create(
        newLineString(0, 0, 1, 1),
        Map.of(),
        NATURAL_EARTH_SOURCE,
        "ne_50m_admin_0_boundary_lines_land",
        1
      )),
      process(SimpleFeature.create(
        newLineString(0, 0, 1, 1),
        Map.of(),
        NATURAL_EARTH_SOURCE,
        "ne_10m_admin_0_boundary_lines_land",
        2
      ))
    );

    assertFeatures(0, List.of(Map.of(
      "_layer", "boundary",
      "_type", "line",
      "disputed", 0,
      "maritime", 0,
      "admin_level", 2,

      "_minzoom", 0,
      "_buffer", 4d
    )), process(SimpleFeature.create(
      newLineString(0, 0, 1, 1),
      Map.of(
        "featurecla", "International boundary (verify)"
      ),
      NATURAL_EARTH_SOURCE,
      "ne_110m_admin_0_boundary_lines_land",
      0
    )));

    assertFeatures(0, List.of(Map.of(
      "_layer", "boundary",
      "_type", "line",
      "disputed", 1,
      "maritime", 0,
      "admin_level", 2,
      "_buffer", 4d
    )), process(SimpleFeature.create(
      newLineString(0, 0, 1, 1),
      Map.of(
        "featurecla", "Disputed (please verify)"
      ),
      NATURAL_EARTH_SOURCE,
      "ne_110m_admin_0_boundary_lines_land",
      0
    )));

    assertFeatures(0, List.of(Map.of(
      "_layer", "boundary",
      "_type", "line",
      "admin_level", 2
    )), process(SimpleFeature.create(
      newLineString(0, 0, 1, 1),
      Map.of(
        "featurecla", "International boundary (verify)"
      ),
      NATURAL_EARTH_SOURCE,
      "ne_50m_admin_0_boundary_lines_land",
      0
    )));

    assertFeatures(0, List.of(Map.of(
      "_layer", "boundary",
      "_type", "line",
      "admin_level", 2
    )), process(SimpleFeature.create(
      newLineString(0, 0, 1, 1),
      Map.of(
        "featurecla", "International boundary (verify)"
      ),
      NATURAL_EARTH_SOURCE,
      "ne_10m_admin_0_boundary_lines_land",
      0
    )));

    assertFeatures(0, List.of(), process(SimpleFeature.create(
      newLineString(0, 0, 1, 1),
      Map.of(
        "featurecla", "Lease Limit"
      ),
      NATURAL_EARTH_SOURCE,
      "ne_10m_admin_0_boundary_lines_land",
      0
    )));
  }

  @Test
  public void testNaturalEarthStateBoundaries() {
    assertFeatures(0, List.of(Map.of(
      "_layer", "boundary",
      "_type", "line",
      "disputed", 0,
      "maritime", 0,
      "admin_level", 4,

      "_minzoom", 1,
      "_maxzoom", 4,
      "_buffer", 4d
    )), process(SimpleFeature.create(
      newLineString(0, 0, 1, 1),
      Map.of(
        "min_zoom", 7d
      ),
      NATURAL_EARTH_SOURCE,
      "ne_10m_admin_1_states_provinces_lines",
      0
    )));
    assertFeatures(0, List.of(Map.of(
      "_layer", "boundary",
      "_type", "line",
      "disputed", 0,
      "maritime", 0,
      "admin_level", 4,

      "_minzoom", 4,
      "_maxzoom", 4,
      "_buffer", 4d
    )), process(SimpleFeature.create(
      newLineString(0, 0, 1, 1),
      Map.of(
        "min_zoom", 7.6d
      ),
      NATURAL_EARTH_SOURCE,
      "ne_10m_admin_1_states_provinces_lines",
      0
    )));

    assertFeatures(0, List.of(), process(SimpleFeature.create(
      newLineString(0, 0, 1, 1),
      Map.of(
        "min_zoom", 7.9d
      ),
      NATURAL_EARTH_SOURCE,
      "ne_10m_admin_1_states_provinces_lines",
      0
    )));

    assertFeatures(0, List.of(), process(SimpleFeature.create(
      newLineString(0, 0, 1, 1),
      Map.of(),
      NATURAL_EARTH_SOURCE,
      "ne_10m_admin_1_states_provinces_lines",
      0
    )));
  }

  @Test
  public void testMergesDisconnectedLineFeatures() throws GeometryException {
    testMergesLinestrings(Map.of("admin_level", 2), Boundary.LAYER_NAME, 10, 13);
    testMergesLinestrings(Map.of("admin_level", 2), Boundary.LAYER_NAME, 10, 14);
  }

  @Test
  public void testOsmTownBoundary() {
    var relation = new OsmElement.Relation(1);
    relation.setTag("type", "boundary");
    relation.setTag("admin_level", "10");
    relation.setTag("boundary", "administrative");

    assertFeatures(14, List.of(Map.of(
      "_layer", "boundary",
      "_type", "line",
      "disputed", 0,
      "maritime", 0,
      "admin_level", 10,

      "_minzoom", 12,
      "_maxzoom", 14,
      "_buffer", 4d,
      "_minpixelsize", 0d
    )), process(lineFeatureWithRelation(
      profile.preprocessOsmRelation(relation),
      Map.of())));
  }

  @Test
  public void testOsmBoundaryLevelTwoAndAHalf() {
    var relation = new OsmElement.Relation(1);
    relation.setTag("type", "boundary");
    relation.setTag("admin_level", "2.5");
    relation.setTag("boundary", "administrative");

    assertFeatures(14, List.of(Map.of(
      "_layer", "boundary",
      "_type", "line",
      "disputed", 0,
      "maritime", 0,
      "admin_level", 3,

      "_minzoom", 5,
      "_maxzoom", 14,
      "_buffer", 4d,
      "_minpixelsize", 0d
    )), process(lineFeatureWithRelation(
      profile.preprocessOsmRelation(relation),
      Map.of())));
  }

  @Test
  public void testOsmBoundaryTakesMinAdminLevel() {
    var relation1 = new OsmElement.Relation(1);
    relation1.setTag("type", "boundary");
    relation1.setTag("admin_level", "10");
    relation1.setTag("name", "Town");
    relation1.setTag("boundary", "administrative");
    var relation2 = new OsmElement.Relation(2);
    relation2.setTag("type", "boundary");
    relation2.setTag("admin_level", "4");
    relation2.setTag("name", "State");
    relation2.setTag("boundary", "administrative");

    assertFeatures(14, List.of(Map.of(
      "_layer", "boundary",
      "_type", "line",
      "disputed", 0,
      "maritime", 0,
      "admin_level", 4
    )), process(lineFeatureWithRelation(
      Stream.concat(
        profile.preprocessOsmRelation(relation2).stream(),
        profile.preprocessOsmRelation(relation1).stream()
      ).toList(),
      Map.of())));
  }

  @Test
  public void testOsmBoundarySetsMaritimeFromWay() {
    var relation1 = new OsmElement.Relation(1);
    relation1.setTag("type", "boundary");
    relation1.setTag("admin_level", "10");
    relation1.setTag("boundary", "administrative");

    assertFeatures(14, List.of(Map.of(
      "maritime", 1
    )), process(lineFeatureWithRelation(
      profile.preprocessOsmRelation(relation1),
      Map.of(
        "maritime", "yes"
      ))
    ));
    assertFeatures(14, List.of(Map.of(
      "maritime", 1
    )), process(lineFeatureWithRelation(
      profile.preprocessOsmRelation(relation1),
      Map.of(
        "natural", "coastline"
      ))
    ));
    assertFeatures(14, List.of(Map.of(
      "maritime", 1
    )), process(lineFeatureWithRelation(
      profile.preprocessOsmRelation(relation1),
      Map.of(
        "boundary_type", "maritime"
      ))
    ));
  }

  @Test
  public void testIgnoresProtectedAreas() {
    var relation1 = new OsmElement.Relation(1);
    relation1.setTag("type", "boundary");
    relation1.setTag("admin_level", "10");
    relation1.setTag("boundary", "protected_area");

    assertNull(profile.preprocessOsmRelation(relation1));
  }

  @Test
  public void testIgnoresProtectedAdminLevelOver10() {
    var relation1 = new OsmElement.Relation(1);
    relation1.setTag("type", "boundary");
    relation1.setTag("admin_level", "11");
    relation1.setTag("boundary", "administrative");

    assertNull(profile.preprocessOsmRelation(relation1));
  }

  @Test
  public void testOsmBoundaryDisputed() {
    var relation = new OsmElement.Relation(1);
    relation.setTag("type", "boundary");
    relation.setTag("admin_level", "5");
    relation.setTag("boundary", "administrative");
    relation.setTag("disputed", "yes");
    relation.setTag("name", "Border A - B");
    relation.setTag("claimed_by", "A");
    assertFeatures(14, List.of(Map.of(
      "_layer", "boundary",
      "_type", "line",
      "disputed_name", "BorderA-B",
      "claimed_by", "A",

      "disputed", 1,
      "maritime", 0,
      "admin_level", 5
    )), process(lineFeatureWithRelation(
      profile.preprocessOsmRelation(relation),
      Map.of())
    ));
  }

  @Test
  public void testOsmBoundaryDisputedFromWay() {
    var relation = new OsmElement.Relation(1);
    relation.setTag("type", "boundary");
    relation.setTag("admin_level", "5");
    relation.setTag("boundary", "administrative");

    assertFeatures(14, List.of(Map.of(
      "_layer", "boundary",
      "_type", "line",

      "disputed", 1,
      "maritime", 0,
      "admin_level", 5
    )), process(lineFeatureWithRelation(
      profile.preprocessOsmRelation(relation),
      Map.of(
        "disputed", "yes"
      ))
    ));

    assertFeatures(14, List.of(Map.of(
      "_layer", "boundary",
      "_type", "line",

      "disputed", 1,
      "maritime", 0,
      "admin_level", 5,
      "claimed_by", "A",
      "disputed_name", "AB"
    )), process(lineFeatureWithRelation(
      profile.preprocessOsmRelation(relation),
      Map.of(
        "disputed", "yes",
        "claimed_by", "A",
        "name", "AB"
      ))
    ));
  }

  @Test
  public void testCountryBoundaryEmittedIfNoName() {
    var relation = new OsmElement.Relation(1);
    relation.setTag("type", "boundary");
    relation.setTag("admin_level", "2");
    relation.setTag("boundary", "administrative");

    assertFeatures(14, List.of(Map.of(
      "_layer", "boundary",
      "_type", "line",

      "disputed", 0,
      "maritime", 0,
      "admin_level", 2
    )), process(lineFeatureWithRelation(
      profile.preprocessOsmRelation(relation),
      Map.of())
    ));
  }

  @Test
  public void testCountryLeftRightName() {
    var country1 = new OsmElement.Relation(1);
    country1.setTag("type", "boundary");
    country1.setTag("admin_level", "2");
    country1.setTag("boundary", "administrative");
    country1.setTag("ISO3166-1:alpha3", "C1");
    var country2 = new OsmElement.Relation(2);
    country2.setTag("type", "boundary");
    country2.setTag("admin_level", "2");
    country2.setTag("boundary", "administrative");
    country2.setTag("ISO3166-1:alpha3", "C2");

    // shared edge
    assertFeatures(14, List.of(), process(SimpleFeature.createFakeOsmFeature(
        newLineString(0, 0, 0, 10),
        Map.of(),
        OSM_SOURCE,
        null,
        3,
        Stream.concat(
          profile.preprocessOsmRelation(country1).stream(),
          profile.preprocessOsmRelation(country2).stream()
        ).map(r -> new OsmReader.RelationMember<>("", r)).toList()
      )
    ));

    // other 2 edges of country 1
    assertFeatures(14, List.of(), process(SimpleFeature.createFakeOsmFeature(
        newLineString(0, 0, 5, 10),
        Map.of(),
        OSM_SOURCE,
        null,
        4,
        profile.preprocessOsmRelation(country1).stream().map(r -> new OsmReader.RelationMember<>("", r))
          .toList()
      )
    ));
    assertFeatures(14, List.of(), process(SimpleFeature.createFakeOsmFeature(
        newLineString(0, 10, 5, 10),
        Map.of(),
        OSM_SOURCE,
        null,
        4,
        profile.preprocessOsmRelation(country1).stream().map(r -> new OsmReader.RelationMember<>("", r))
          .toList()
      )
    ));

    // other 2 edges of country 2
    assertFeatures(14, List.of(), process(SimpleFeature.createFakeOsmFeature(
        newLineString(0, 0, -5, 10),
        Map.of(),
        OSM_SOURCE,
        null,
        4,
        profile.preprocessOsmRelation(country2).stream().map(r -> new OsmReader.RelationMember<>("", r))
          .toList()
      )
    ));
    assertFeatures(14, List.of(), process(SimpleFeature.createFakeOsmFeature(
        newLineString(0, 10, -5, 10),
        Map.of(),
        OSM_SOURCE,
        null,
        4,
        profile.preprocessOsmRelation(country2).stream().map(r -> new OsmReader.RelationMember<>("", r))
          .toList()
      )
    ));

    List<FeatureCollector.Feature> features = new ArrayList<>();
    profile.finish(OSM_SOURCE, new FeatureCollector.Factory(params, stats), features::add);
    assertEquals(3, features.size());

    // ensure shared edge has country labels on right sides
    var sharedEdge = features.stream()
      .filter(c -> c.getAttrsAtZoom(0).containsKey("adm0_l") && c.getAttrsAtZoom(0).containsKey("adm0_r")).findFirst()
      .get();
    if (sharedEdge.getGeometry().getCoordinate().y == 0.5) { // going up
      assertEquals("C1", sharedEdge.getAttrsAtZoom(0).get("adm0_r"));
      assertEquals("C2", sharedEdge.getAttrsAtZoom(0).get("adm0_l"));
    } else { // going down
      assertEquals("C2", sharedEdge.getAttrsAtZoom(0).get("adm0_r"));
      assertEquals("C1", sharedEdge.getAttrsAtZoom(0).get("adm0_l"));
    }
    var c1 = features.stream()
      .filter(c -> c.getGeometry().getEnvelopeInternal().getMaxX() > 0.5).findFirst()
      .get();
    if (c1.getGeometry().getCoordinate().y == 0.5) { // going up
      assertEquals("C1", c1.getAttrsAtZoom(0).get("adm0_l"));
    } else { // going down
      assertEquals("C1", c1.getAttrsAtZoom(0).get("adm0_r"));
    }
    var c2 = features.stream()
      .filter(c -> c.getGeometry().getEnvelopeInternal().getMinX() < 0.5).findFirst()
      .get();
    if (c2.getGeometry().getCoordinate().y == 0.5) { // going up
      assertEquals("C2", c2.getAttrsAtZoom(0).get("adm0_r"));
    } else { // going down
      assertEquals("C2", c2.getAttrsAtZoom(0).get("adm0_l"));
    }
  }

  @Test
  public void testCountryBoundaryNotClosed() {
    var country1 = new OsmElement.Relation(1);
    country1.setTag("type", "boundary");
    country1.setTag("admin_level", "2");
    country1.setTag("boundary", "administrative");
    country1.setTag("ISO3166-1:alpha3", "C1");

    // shared edge
    assertFeatures(14, List.of(), process(SimpleFeature.createFakeOsmFeature(
      newLineString(0, 0, 0, 10, 5, 5),
      Map.of(),
      OSM_SOURCE,
      null,
      3,
      profile.preprocessOsmRelation(country1).stream().map(r -> new OsmReader.RelationMember<>("", r))
        .toList()
    )));

    List<FeatureCollector.Feature> features = new ArrayList<>();
    profile.finish(OSM_SOURCE, new FeatureCollector.Factory(params, stats), features::add);
    assertFeatures(0, List.of(Map.of(
      "adm0_r", "<null>",
      "adm0_l", "<null>",
      "maritime", 0,
      "disputed", 0,
      "admin_level", 2,

      "_layer", "boundary"
    )), features);
  }

  @Test
  public void testNestedCountry() throws GeometryException {
    var country1 = new OsmElement.Relation(1);
    country1.setTag("type", "boundary");
    country1.setTag("admin_level", "2");
    country1.setTag("boundary", "administrative");
    country1.setTag("ISO3166-1:alpha3", "C1");

    assertFeatures(14, List.of(), process(SimpleFeature.createFakeOsmFeature(
      GeoUtils.polygonToLineString(rectangle(0, 10)),
      Map.of(),
      OSM_SOURCE,
      null,
      3,
      profile.preprocessOsmRelation(country1).stream().map(r -> new OsmReader.RelationMember<>("", r))
        .toList()
    )));
    assertFeatures(14, List.of(), process(SimpleFeature.createFakeOsmFeature(
      GeoUtils.polygonToLineString(rectangle(1, 9)),
      Map.of(),
      OSM_SOURCE,
      null,
      3,
      profile.preprocessOsmRelation(country1).stream().map(r -> new OsmReader.RelationMember<>("", r))
        .toList()
    )));

    List<FeatureCollector.Feature> features = new ArrayList<>();
    profile.finish(OSM_SOURCE, new FeatureCollector.Factory(params, stats), features::add);
    assertFeatures(0, List.of(Map.of(
      "adm0_l", "C1",
      "adm0_r", "<null>"
    ), Map.of(
      "adm0_r", "C1",
      "adm0_l", "<null>"
    )), features);
  }

  @Test
  public void testDontLabelBadPolygon() {
    var country1 = new OsmElement.Relation(1);
    country1.setTag("type", "boundary");
    country1.setTag("admin_level", "2");
    country1.setTag("boundary", "administrative");
    country1.setTag("ISO3166-1:alpha3", "C1");

    assertFeatures(14, List.of(), process(SimpleFeature.createFakeOsmFeature(
      GeoUtils.worldToLatLonCoords(newLineString(0, 0, 0.1, 0, 0.1, 0.1, 0.02, 0.1, 0.02, -0.02)),
      Map.of(),
      OSM_SOURCE,
      null,
      3,
      profile.preprocessOsmRelation(country1).stream().map(r -> new OsmReader.RelationMember<>("", r))
        .toList()
    )));

    List<FeatureCollector.Feature> features = new ArrayList<>();
    profile.finish(OSM_SOURCE, new FeatureCollector.Factory(params, stats), features::add);
    assertFeatures(0, List.of(Map.of(
      "adm0_l", "<null>",
      "adm0_r", "<null>"
    )), features);
  }

  @Test
  public void testIgnoreBadPolygonAndLabelGoodPart() throws GeometryException {
    var country1 = new OsmElement.Relation(1);
    country1.setTag("type", "boundary");
    country1.setTag("admin_level", "2");
    country1.setTag("boundary", "administrative");
    country1.setTag("ISO3166-1:alpha3", "C1");

    assertFeatures(14, List.of(), process(SimpleFeature.createFakeOsmFeature(
      GeoUtils.worldToLatLonCoords(newLineString(0, 0, 0.1, 0, 0.1, 0.1, 0.2, 0.1, 0.2, -0.2)),
      Map.of(),
      OSM_SOURCE,
      null,
      3,
      profile.preprocessOsmRelation(country1).stream().map(r -> new OsmReader.RelationMember<>("", r))
        .toList()
    )));

    assertFeatures(14, List.of(), process(SimpleFeature.createFakeOsmFeature(
      GeoUtils.worldToLatLonCoords(GeoUtils.polygonToLineString(rectangle(0.2, 0.3))),
      Map.of(),
      OSM_SOURCE,
      null,
      3,
      profile.preprocessOsmRelation(country1).stream().map(r -> new OsmReader.RelationMember<>("", r))
        .toList()
    )));

    List<FeatureCollector.Feature> features = new ArrayList<>();
    profile.finish(OSM_SOURCE, new FeatureCollector.Factory(params, stats), features::add);
    assertFeatures(0, List.of(Map.of(
      "adm0_l", "<null>",
      "adm0_r", "<null>"
    ), Map.of(
      "adm0_l", "<null>",
      "adm0_r", "C1"
    )), features);
  }

}
