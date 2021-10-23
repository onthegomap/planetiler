package com.onthegomap.flatmap.basemap.layers;

import static com.onthegomap.flatmap.TestUtils.newLineString;
import static com.onthegomap.flatmap.TestUtils.rectangle;
import static com.onthegomap.flatmap.basemap.BasemapProfile.NATURAL_EARTH_SOURCE;
import static com.onthegomap.flatmap.basemap.BasemapProfile.OSM_SOURCE;

import com.onthegomap.flatmap.FeatureCollector;
import com.onthegomap.flatmap.geo.GeometryException;
import com.onthegomap.flatmap.reader.SimpleFeature;
import com.onthegomap.flatmap.reader.osm.OsmElement;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;

public class TransportationTest extends AbstractLayerTest {

  @Test
  public void testNamedFootway() {
    FeatureCollector result = process(lineFeature(Map.of(
      "name", "Lagoon Path",
      "surface", "asphalt",
      "level", "0",
      "highway", "footway",
      "indoor", "no",
      "oneway", "no",
      "foot", "designated",
      "bicycle", "dismount"
    )));
    assertFeatures(13, List.of(Map.of(
      "_layer", "transportation",
      "_type", "line",
      "class", "path",
      "subclass", "footway",
      "oneway", 0,
      "name", "<null>",
      "_buffer", 4d,
      "_minpixelsize", 0d,
      "_minzoom", 13,
      "_maxzoom", 14
    ), Map.of(
      "_layer", "transportation_name",
      "_type", "line",
      "class", "path",
      "subclass", "footway",
      "name", "Lagoon Path",
      "name_int", "Lagoon Path",
      "name:latin", "Lagoon Path",
      "_minpixelsize", 0d,
      "_minzoom", 13,
      "_maxzoom", 14
    )), result);
    assertFeatures(13, List.of(Map.of(
      "_layer", "transportation",
      "surface", "paved",
      "oneway", 0,
      "level", 0L,
      "ramp", 0,
      "bicycle", "dismount",
      "foot", "designated"
    ), Map.of(
      "_layer", "transportation_name",
      "level", 0L,
      "surface", "<null>",
      "oneway", "<null>",
      "ramp", "<null>",
      "bicycle", "<null>",
      "foot", "<null>"
    )), result);
  }

  @Test
  public void testUnnamedPath() {
    assertFeatures(13, List.of(Map.of(
      "_layer", "transportation",
      "class", "path",
      "subclass", "path",
      "surface", "unpaved",
      "oneway", 0
    )), process(lineFeature(Map.of(
      "surface", "dirt",
      "highway", "path"
    ))));
  }

  @Test
  public void testIndoorTunnelSteps() {
    assertFeatures(13, List.of(Map.of(
      "_layer", "transportation",
      "class", "path",
      "subclass", "steps",
      "brunnel", "tunnel",
      "indoor", 1,
      "oneway", 1,
      "ramp", 1
    )), process(lineFeature(Map.of(
      "highway", "steps",
      "tunnel", "building_passage",
      "oneway", "yes",
      "indoor", "yes"
    ))));
  }

  @Test
  public void testInterstateMotorway() {
    var rel = new OsmElement.Relation(1);
    rel.setTag("type", "route");
    rel.setTag("route", "road");
    rel.setTag("network", "US:I");
    rel.setTag("ref", "90");

    FeatureCollector features = process(lineFeatureWithRelation(
      profile.preprocessOsmRelation(rel),
      Map.of(
        "highway", "motorway",
        "oneway", "yes",
        "name", "Massachusetts Turnpike",
        "ref", "I 90",
        "surface", "asphalt",
        "foot", "no",
        "bicycle", "no",
        "horse", "no",
        "bridge", "yes"
      )));

    assertFeatures(13, List.of(Map.of(
      "_layer", "transportation",
      "class", "motorway",
      "surface", "paved",
      "oneway", 1,
      "ramp", 0,
      "bicycle", "no",
      "foot", "no",
      "horse", "no",
      "brunnel", "bridge",
      "_minzoom", 4
    ), Map.of(
      "_layer", "transportation_name",
      "class", "motorway",
      "name", "Massachusetts Turnpike",
      "name_en", "Massachusetts Turnpike",
      "ref", "90",
      "ref_length", 2,
      "network", "us-interstate",
      "brunnel", "<null>",
      "_minzoom", 6
    )), features);

    assertFeatures(8, List.of(Map.of(
      "_layer", "transportation",
      "class", "motorway",
      "surface", "<null>",
      "oneway", "<null>",
      "ramp", "<null>",
      "bicycle", "<null>",
      "foot", "<null>",
      "horse", "<null>",
      "brunnel", "bridge",
      "_minzoom", 4
    ), Map.of(
      "_layer", "transportation_name",
      "class", "motorway",
      "name", "Massachusetts Turnpike",
      "name_en", "Massachusetts Turnpike",
      "ref", "90",
      "ref_length", 2,
      "network", "us-interstate",
      "brunnel", "<null>",
      "_minzoom", 6
    )), features);
  }

  @Test
  public void testPrimaryRoadConstruction() {
    assertFeatures(13, List.of(Map.of(
      "_layer", "transportation",
      "class", "primary_construction",
      "brunnel", "bridge",
      "layer", 1L,
      "oneway", 1,
      "_minzoom", 7
    ), Map.of(
      "_layer", "transportation_name",
      "name", "North Washington Street",
      "class", "primary_construction",
      "brunnel", "<null>",
      "_minzoom", 12
    )), process(lineFeature(Map.of(
      "highway", "construction",
      "construction", "primary",
      "bridge", "yes",
      "layer", "1",
      "name", "North Washington Street",
      "oneway", "yes"
    ))));
  }

  @Test
  public void testRaceway() {
    assertFeatures(13, List.of(Map.of(
      "_layer", "transportation",
      "class", "raceway",
      "oneway", 1,
      "_minzoom", 12
    ), Map.of(
      "_layer", "transportation_name",
      "class", "raceway",
      "name", "Climbing Turn",
      "ref", "5",
      "_minzoom", 12
    )), process(lineFeature(Map.of(
      "highway", "raceway",
      "oneway", "yes",
      "ref", "5",
      "name", "Climbing Turn"
    ))));
  }

  @Test
  public void testDriveway() {
    assertFeatures(13, List.of(Map.of(
      "_layer", "transportation",
      "class", "service",
      "service", "driveway",
      "_minzoom", 12
    )), process(lineFeature(Map.of(
      "highway", "service",
      "service", "driveway"
    ))));
  }

  @Test
  public void testMountainBikeTrail() {
    assertFeatures(13, List.of(Map.of(
      "_layer", "transportation",
      "class", "path",
      "subclass", "path",
      "mtb_scale", "4",
      "surface", "unpaved",
      "bicycle", "yes",
      "_minzoom", 13
    ), Map.of(
      "_layer", "transportation_name",
      "class", "path",
      "subclass", "path",
      "name", "Path name",
      "_minzoom", 13
    )), process(lineFeature(Map.of(
      "highway", "path",
      "mtb:scale", "4",
      "name", "Path name",
      "bicycle", "yes",
      "surface", "ground"
    ))));
  }

  @Test
  public void testTrack() {
    assertFeatures(13, List.of(Map.of(
      "_layer", "transportation",
      "class", "track",
      "surface", "unpaved",
      "horse", "yes",
      "_minzoom", 14
    )), process(lineFeature(Map.of(
      "highway", "track",
      "surface", "dirt",
      "horse", "yes"
    ))));
  }

  final OsmElement.Relation relUS = new OsmElement.Relation(1);

  {
    relUS.setTag("type", "route");
    relUS.setTag("route", "road");
    relUS.setTag("network", "US:US");
    relUS.setTag("ref", "3");
  }

  final OsmElement.Relation relMA = new OsmElement.Relation(2);

  {
    relMA.setTag("type", "route");
    relMA.setTag("route", "road");
    relMA.setTag("network", "US:MA");
    relMA.setTag("ref", "2");
  }

  @Test
  public void testUSAndStateHighway() {
    assertFeatures(13, List.of(Map.of(
      "_layer", "transportation",
      "class", "primary",
      "surface", "paved",
      "oneway", 0,
      "ramp", 0,
      "_minzoom", 7
    ), Map.of(
      "_layer", "transportation_name",
      "class", "primary",
      "name", "Memorial Drive",
      "name_en", "Memorial Drive",
      "ref", "3",
      "ref_length", 1,
      "network", "us-highway",
      "_minzoom", 12
    )), process(lineFeatureWithRelation(
      Stream.concat(
        profile.preprocessOsmRelation(relUS).stream(),
        profile.preprocessOsmRelation(relMA).stream()
      ).toList(),
      Map.of(
        "highway", "primary",
        "name", "Memorial Drive",
        "ref", "US 3;MA 2",
        "surface", "asphalt"
      ))));

    // swap order
    assertFeatures(13, List.of(Map.of(
      "_layer", "transportation",
      "class", "primary"
    ), Map.of(
      "_layer", "transportation_name",
      "class", "primary",
      "ref", "3",
      "network", "us-highway"
    )), process(lineFeatureWithRelation(
      Stream.concat(
        profile.preprocessOsmRelation(relMA).stream(),
        profile.preprocessOsmRelation(relUS).stream()
      ).toList(),
      Map.of(
        "highway", "primary",
        "name", "Memorial Drive",
        "ref", "US 3;MA 2",
        "surface", "asphalt"
      ))));
  }

  @Test
  public void testUsStateHighway() {
    assertFeatures(13, List.of(Map.of(
      "_layer", "transportation",
      "class", "primary"
    ), Map.of(
      "_layer", "transportation_name",
      "class", "primary",
      "name", "Memorial Drive",
      "name_en", "Memorial Drive",
      "ref", "2",
      "ref_length", 1,
      "network", "us-state",
      "_minzoom", 12
    )), process(lineFeatureWithRelation(
      profile.preprocessOsmRelation(relMA),
      Map.of(
        "highway", "primary",
        "name", "Memorial Drive",
        "ref", "US 3;MA 2",
        "surface", "asphalt"
      ))));
  }

  @Test
  public void testCompoundRef() {
    assertFeatures(13, List.of(Map.of(
      "_layer", "transportation",
      "class", "primary"
    ), Map.of(
      "_layer", "transportation_name",
      "class", "primary",
      "name", "Memorial Drive",
      "name_en", "Memorial Drive",
      "ref", "US 3;MA 2",
      "ref_length", 9,
      "network", "road",
      "_minzoom", 12
    )), process(lineFeature(
      Map.of(
        "highway", "primary",
        "name", "Memorial Drive",
        "ref", "US 3;MA 2",
        "surface", "asphalt"
      ))));
  }

  @Test
  public void testTransCanadaHighway() {
    var rel = new OsmElement.Relation(1);
    rel.setTag("type", "route");
    rel.setTag("route", "road");
    rel.setTag("network", "CA:transcanada:namedRoute");

    FeatureCollector features = process(lineFeatureWithRelation(
      profile.preprocessOsmRelation(rel),
      Map.of(
        "highway", "motorway",
        "oneway", "yes",
        "name", "Autoroute Claude-Béchard",
        "ref", "85",
        "surface", "asphalt"
      )));

    assertFeatures(13, List.of(Map.of(
      "_layer", "transportation",
      "class", "motorway",
      "surface", "paved",
      "oneway", 1,
      "ramp", 0,
      "_minzoom", 4
    ), Map.of(
      "_layer", "transportation_name",
      "class", "motorway",
      "name", "Autoroute Claude-Béchard",
      "name_en", "Autoroute Claude-Béchard",
      "ref", "85",
      "ref_length", 2,
      "network", "ca-transcanada",
      "_minzoom", 6
    )), features);
  }

  @Test
  public void testGreatBritainHighway() {
    process(SimpleFeature.create(
      rectangle(0, 0.1),
      Map.of("iso_a2", "GB"),
      NATURAL_EARTH_SOURCE,
      "ne_10m_admin_0_countries",
      0
    ));

    // in GB
    assertFeatures(13, List.of(Map.of(
      "_layer", "transportation",
      "class", "motorway",
      "oneway", 1,
      "ramp", 0,
      "_minzoom", 4
    ), Map.of(
      "_layer", "transportation_name",
      "class", "motorway",
      "ref", "M1",
      "ref_length", 2,
      "network", "gb-motorway",
      "_minzoom", 6
    )), process(SimpleFeature.create(
      newLineString(0, 0, 1, 1),
      Map.of(
        "highway", "motorway",
        "oneway", "yes",
        "ref", "M1"
      ),
      OSM_SOURCE,
      null,
      0
    )));

    // not in GB
    assertFeatures(13, List.of(Map.of(
      "_layer", "transportation",
      "class", "motorway",
      "oneway", 1,
      "ramp", 0,
      "_minzoom", 4
    ), Map.of(
      "_layer", "transportation_name",
      "class", "motorway",
      "ref", "M1",
      "ref_length", 2,
      "network", "road",
      "_minzoom", 6
    )), process(SimpleFeature.create(
      newLineString(1, 0, 0, 1),
      Map.of(
        "highway", "motorway",
        "oneway", "yes",
        "ref", "M1"
      ),
      OSM_SOURCE,
      null,
      0
    )));
  }

  @Test
  public void testMergesDisconnectedRoadFeatures() throws GeometryException {
    testMergesLinestrings(Map.of("class", "motorway"), Transportation.LAYER_NAME, 10, 14);
  }

  @Test
  public void testMergesDisconnectedRoadNameFeatures() throws GeometryException {
    testMergesLinestrings(Map.of("class", "motorway"), TransportationName.LAYER_NAME, 10, 14);
  }

  @Test
  public void testLightRail() {
    assertFeatures(13, List.of(Map.of(
      "_layer", "transportation",
      "class", "transit",
      "subclass", "light_rail",
      "brunnel", "tunnel",
      "layer", -1L,
      "oneway", 0,
      "ramp", 0,

      "_minzoom", 11,
      "_maxzoom", 14,
      "_type", "line"
    )), process(lineFeature(Map.of(
      "railway", "light_rail",
      "name", "Green Line",
      "tunnel", "yes",
      "layer", "-1"
    ))));
  }

  @Test
  public void testSubway() {
    assertFeatures(13, List.of(Map.of(
      "_layer", "transportation",
      "class", "transit",
      "subclass", "subway",
      "brunnel", "tunnel",
      "layer", -2L,
      "oneway", 0,
      "ramp", 0,

      "_minzoom", 14,
      "_maxzoom", 14,
      "_type", "line"
    )), process(lineFeature(Map.of(
      "railway", "subway",
      "name", "Red Line",
      "tunnel", "yes",
      "layer", "-2",
      "level", "-2"
    ))));
  }

  @Test
  public void testRail() {
    assertFeatures(8, List.of(Map.of(
      "_layer", "transportation",
      "class", "rail",
      "subclass", "rail",
      "brunnel", "<null>",
      "layer", "<null>",

      "_minzoom", 8,
      "_maxzoom", 14,
      "_type", "line"
    )), process(lineFeature(Map.of(
      "railway", "rail",
      "name", "Boston Subdivision",
      "usage", "main",
      "tunnel", "yes",
      "layer", "-2"
    ))));
    assertFeatures(13, List.of(Map.of(
      "_minzoom", 10
    )), process(lineFeature(Map.of(
      "railway", "rail",
      "name", "Boston Subdivision"
    ))));
    assertFeatures(13, List.of(),
      process(polygonFeature(Map.of(
        "railway", "rail"
      ))));
    assertFeatures(13, List.of(Map.of(
      "class", "rail",
      "subclass", "rail",
      "_minzoom", 14,
      "service", "yard"
    )), process(lineFeature(Map.of(
      "railway", "rail",
      "name", "Boston Subdivision",
      "service", "yard"
    ))));
  }

  @Test
  public void testNarrowGauge() {
    assertFeatures(10, List.of(Map.of(
      "_layer", "transportation",
      "class", "rail",
      "subclass", "narrow_gauge",

      "_minzoom", 10,
      "_maxzoom", 14,
      "_type", "line"
    )), process(lineFeature(Map.of(
      "railway", "narrow_gauge"
    ))));
  }

  @Test
  public void testAerialway() {
    assertFeatures(10, List.of(Map.of(
      "_layer", "transportation",
      "class", "aerialway",
      "subclass", "gondola",

      "_minzoom", 12,
      "_maxzoom", 14,
      "_type", "line"
    )), process(lineFeature(Map.of(
      "aerialway", "gondola",
      "name", "Summit Gondola"
    ))));
    assertFeatures(10, List.of(),
      process(polygonFeature(Map.of(
        "aerialway", "gondola",
        "name", "Summit Gondola"
      ))));
  }

  @Test
  public void testFerry() {
    assertFeatures(10, List.of(Map.of(
      "_layer", "transportation",
      "class", "ferry",

      "_minzoom", 11,
      "_maxzoom", 14,
      "_type", "line"
    )), process(lineFeature(Map.of(
      "route", "ferry",
      "name", "Boston - Provincetown Ferry",
      "motor_vehicle", "no",
      "foot", "yes",
      "bicycle", "yes"
    ))));
    assertFeatures(10, List.of(),
      process(polygonFeature(Map.of(
        "route", "ferry",
        "name", "Boston - Provincetown Ferry",
        "motor_vehicle", "no",
        "foot", "yes",
        "bicycle", "yes"
      ))));
  }

  @Test
  public void testPiers() {
    // area
    assertFeatures(10, List.of(Map.of(
      "_layer", "transportation",
      "class", "pier",

      "_minzoom", 13,
      "_maxzoom", 14,
      "_type", "polygon"
    )), process(polygonFeature(Map.of(
      "man_made", "pier"
    ))));
    assertFeatures(10, List.of(Map.of(
      "_layer", "transportation",
      "class", "pier",

      "_minzoom", 13,
      "_maxzoom", 14,
      "_type", "line"
    )), process(lineFeature(Map.of(
      "man_made", "pier"
    ))));
  }

  @Test
  public void testPedestrianArea() {
    assertFeatures(10, List.of(Map.of(
      "_layer", "transportation",
      "class", "path",
      "subclass", "pedestrian",

      "_minzoom", 13,
      "_maxzoom", 14,
      "_type", "polygon"
    )), process(polygonFeature(Map.of(
      "highway", "pedestrian",
      "area", "yes",
      "foot", "yes"
    ))));
    // ignore underground pedestrian areas
    assertFeatures(10, List.of(),
      process(polygonFeature(Map.of(
        "highway", "pedestrian",
        "area", "yes",
        "foot", "yes",
        "layer", "-1"
      ))));
  }

  private int getWaySortKey(Map<String, Object> tags) {
    var iter = process(lineFeature(tags)).iterator();
    return iter.next().getSortKey();
  }

  @Test
  public void testSortKeys() {
    assertDescending(
      getWaySortKey(Map.of("highway", "footway", "layer", "2")),
      getWaySortKey(Map.of("highway", "motorway", "bridge", "yes")),
      getWaySortKey(Map.of("highway", "footway", "bridge", "yes")),
      getWaySortKey(Map.of("highway", "motorway")),
      getWaySortKey(Map.of("highway", "trunk")),
      getWaySortKey(Map.of("railway", "rail")),
      getWaySortKey(Map.of("highway", "primary")),
      getWaySortKey(Map.of("highway", "secondary")),
      getWaySortKey(Map.of("highway", "tertiary")),
      getWaySortKey(Map.of("highway", "motorway_link")),
      getWaySortKey(Map.of("highway", "footway")),
      getWaySortKey(Map.of("highway", "motorway", "tunnel", "yes")),
      getWaySortKey(Map.of("highway", "footway", "tunnel", "yes")),
      getWaySortKey(Map.of("highway", "motorway", "layer", "-2"))
    );
  }
}
