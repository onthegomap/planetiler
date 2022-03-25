package com.onthegomap.planetiler.basemap.layers;

import static com.onthegomap.planetiler.TestUtils.newLineString;
import static com.onthegomap.planetiler.TestUtils.newPoint;
import static com.onthegomap.planetiler.TestUtils.rectangle;
import static com.onthegomap.planetiler.basemap.BasemapProfile.NATURAL_EARTH_SOURCE;
import static com.onthegomap.planetiler.basemap.BasemapProfile.OSM_SOURCE;

import com.onthegomap.planetiler.FeatureCollector;
import com.onthegomap.planetiler.basemap.BasemapProfile;
import com.onthegomap.planetiler.config.Arguments;
import com.onthegomap.planetiler.config.PlanetilerConfig;
import com.onthegomap.planetiler.geo.GeometryException;
import com.onthegomap.planetiler.reader.SimpleFeature;
import com.onthegomap.planetiler.reader.SourceFeature;
import com.onthegomap.planetiler.reader.osm.OsmElement;
import com.onthegomap.planetiler.stats.Stats;
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
      "oneway", "<null>",
      "name", "<null>",
      "layer", "<null>",
      "_buffer", 4d,
      "_minpixelsize", 0d,
      "_minzoom", 13
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
      "oneway", "<null>",
      "layer", "<null>",
      "level", 0L,
      "ramp", "<null>",
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
  public void testImportantPath() {
    var rel = new OsmElement.Relation(1);

    rel.setTag("colour", "white");
    rel.setTag("name", "Appalachian Trail - 11 MA");
    rel.setTag("network", "nwn");
    rel.setTag("osmc", "symbol	white::white_stripe");
    rel.setTag("ref", "AT");
    rel.setTag("route", "hiking");
    rel.setTag("short_name", "AT 11 MA");
    rel.setTag("symbol", "white-paint blazes");
    rel.setTag("type", "route");
    rel.setTag("wikidata", "Q620648");
    rel.setTag("wikipedia", "en:Appalachian Trail");

    FeatureCollector features = process(lineFeatureWithRelation(
      profile.preprocessOsmRelation(rel),
      Map.of(
        "bicycle", "no",
        "highway", "path",
        "horse", "no",
        "name", "Appalachian Trail",
        "ref", "AT",
        "surface", "ground"
      )));
    assertFeatures(12, List.of(Map.of(
      "_layer", "transportation",
      "_type", "line",
      "class", "path",
      "subclass", "path",
      "oneway", "<null>",
      "name", "<null>",
      "layer", "<null>",
      "_buffer", 4d,
      "_minpixelsize", 0d,
      "_minzoom", 12
    ), Map.of(
      "_layer", "transportation_name",
      "_type", "line",
      "class", "path",
      "subclass", "path",
      "name", "Appalachian Trail",
      "name_int", "Appalachian Trail",
      "name:latin", "Appalachian Trail",
      "_minpixelsize", 0d,
      "_minzoom", 12
    )), features);
  }

  @Test
  public void testUnnamedPath() {
    assertFeatures(14, List.of(Map.of(
      "_layer", "transportation",
      "class", "path",
      "subclass", "path",
      "surface", "unpaved",
      "oneway", "<null>",
      "_minzoom", 14
    )), process(lineFeature(Map.of(
      "surface", "dirt",
      "highway", "path"
    ))));
  }

  @Test
  public void testPrivatePath() {
    assertFeatures(9, List.of(Map.of(
      "_layer", "transportation",
      "class", "path",
      "access", "no"
    )), process(lineFeature(Map.of(
      "access", "private",
      "highway", "path"
    ))));
    assertFeatures(9, List.of(Map.of(
      "_layer", "transportation",
      "class", "path",
      "access", "no"
    )), process(lineFeature(Map.of(
      "access", "no",
      "highway", "path"
    ))));
    assertFeatures(8, List.of(Map.of(
      "_layer", "transportation",
      "class", "path",
      "access", "<null>"
    )), process(lineFeature(Map.of(
      "access", "no",
      "highway", "path"
    ))));
  }

  @Test
  public void testExpressway() {
    assertFeatures(8, List.of(Map.of(
      "_layer", "transportation",
      "class", "motorway",
      "expressway", "<null>"
    )), process(lineFeature(Map.of(
      "highway", "motorway",
      "expressway", "yes"
    ))));
    assertFeatures(8, List.of(Map.of(
      "_layer", "transportation",
      "class", "primary",
      "expressway", 1
    )), process(lineFeature(Map.of(
      "highway", "primary",
      "expressway", "yes"
    ))));
    assertFeatures(7, List.of(Map.of(
      "_layer", "transportation",
      "class", "primary",
      "expressway", "<null>"
    )), process(lineFeature(Map.of(
      "highway", "primary",
      "expressway", "yes"
    ))));
  }

  @Test
  public void testToll() {
    assertFeatures(9, List.of(Map.of(
      "_layer", "transportation",
      "class", "motorway",
      "toll", "<null>"
    )), process(lineFeature(Map.of(
      "highway", "motorway"
    ))));
    assertFeatures(9, List.of(Map.of(
      "_layer", "transportation",
      "class", "motorway",
      "toll", 1
    )), process(lineFeature(Map.of(
      "highway", "motorway",
      "toll", "yes"
    ))));
    assertFeatures(8, List.of(Map.of(
      "_layer", "transportation",
      "class", "motorway",
      "toll", "<null>"
    )), process(lineFeature(Map.of(
      "highway", "motorway",
      "toll", "yes"
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
      "ramp", "<null>"
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

    assertFeatures(13, List.of(mapOf(
      "_layer", "transportation",
      "class", "motorway",
      "surface", "paved",
      "oneway", 1,
      "ramp", "<null>",
      "bicycle", "no",
      "foot", "no",
      "horse", "no",
      "brunnel", "bridge",
      "network", "us-interstate",
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
      "route_1", "US:I=90",
      "_minzoom", 6
    )), features);

    assertFeatures(13, List.of(Map.of(
      "_layer", "transportation",
      "class", "motorway",
      "surface", "paved",
      "oneway", 1,
      "ramp", "<null>",
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
      "route_1", "US:I=90",
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
      "route_1", "US:I=90",
      "brunnel", "<null>",
      "_minzoom", 6
    )), features);
  }

  @Test
  public void testRouteWithoutNetworkType() {
    var rel1 = new OsmElement.Relation(1);
    rel1.setTag("type", "route");
    rel1.setTag("route", "road");
    rel1.setTag("network", "US:NJ:NJTP");
    rel1.setTag("ref", "NJTP");
    rel1.setTag("name", "New Jersey Turnpike (mainline)");

    var rel2 = new OsmElement.Relation(1);
    rel2.setTag("type", "route");
    rel2.setTag("route", "road");
    rel2.setTag("network", "US:I");
    rel2.setTag("ref", "95");
    rel2.setTag("name", "I 95 (NJ)");

    FeatureCollector rendered = process(lineFeatureWithRelation(
      Stream.concat(
        profile.preprocessOsmRelation(rel1).stream(),
        profile.preprocessOsmRelation(rel2).stream()
      ).toList(),
      Map.of(
        "highway", "motorway",
        "name", "New Jersey Turnpike",
        "ref", "I 95;NJTP"
      )));

    assertFeatures(13, List.of(mapOf(
      "_layer", "transportation",
      "class", "motorway",
      "_minzoom", 4
    ), Map.of(
      "_layer", "transportation_name",
      "class", "motorway",
      "name", "New Jersey Turnpike",
      "ref", "95",
      "ref_length", 2,
      "route_1", "US:I=95",
      "route_2", "US:NJ:NJTP=NJTP",
      "_minzoom", 6
    )), rendered);
  }

  @Test
  public void testPolishHighwayIssue165() {
    var rel1 = new OsmElement.Relation(1);
    rel1.setTag("type", "route");
    rel1.setTag("route", "road");
    rel1.setTag("network", "e-road");
    rel1.setTag("ref", "E 77");
    rel1.setTag("name", "European route E 77");

    var rel2 = new OsmElement.Relation(2);
    rel2.setTag("type", "route");
    rel2.setTag("route", "road");
    rel2.setTag("network", "e-road");
    rel2.setTag("ref", "E 28");
    rel2.setTag("name", "European route E 28");

    FeatureCollector rendered = process(lineFeatureWithRelation(
      Stream.concat(
        profile.preprocessOsmRelation(rel1).stream(),
        profile.preprocessOsmRelation(rel2).stream()
      ).toList(),
      Map.of(
        "highway", "trunk",
        "ref", "S7"
      )));

    assertFeatures(13, List.of(mapOf(
      "_layer", "transportation",
      "class", "trunk"
    ), Map.of(
      "_layer", "transportation_name",
      "class", "trunk",
      "name", "<null>",
      "ref", "S7",
      "ref_length", 2,
      "route_1", "e-road=E 28",
      "route_2", "e-road=E 77"
    )), rendered);
  }

  @Test
  public void testMotorwayJunction() {
    var otherNode1 = new OsmElement.Node(1, 1, 1);
    var junctionNode = new OsmElement.Node(2, 1, 2);
    var otherNode2 = new OsmElement.Node(3, 1, 3);
    var otherNode3 = new OsmElement.Node(4, 2, 3);

    junctionNode.setTag("highway", "motorway_junction");
    junctionNode.setTag("name", "exit 1");
    junctionNode.setTag("layer", "1");
    junctionNode.setTag("ref", "12");

    // 2 ways meet at junctionNode (id=2) - use most important class of a highway intersecting it (motorway)
    var way1 = new OsmElement.Way(5);
    way1.setTag("highway", "motorway");
    way1.nodes().add(otherNode1.id(), junctionNode.id(), otherNode2.id());
    var way2 = new OsmElement.Way(6);
    way2.setTag("highway", "primary");
    way2.nodes().add(junctionNode.id(), otherNode3.id());

    profile.preprocessOsmNode(otherNode1);
    profile.preprocessOsmNode(junctionNode);
    profile.preprocessOsmNode(otherNode2);
    profile.preprocessOsmNode(otherNode3);

    profile.preprocessOsmWay(way1);
    profile.preprocessOsmWay(way2);

    FeatureCollector features = process(SimpleFeature.create(
      newPoint(1, 2),
      junctionNode.tags(),
      OSM_SOURCE,
      null,
      junctionNode.id()
    ));

    assertFeatures(13, List.of(mapOf(
      "_layer", "transportation_name",
      "class", "motorway",
      "subclass", "junction",
      "name", "exit 1",
      "ref", "12",
      "ref_length", 2,
      "layer", 1L,
      "_type", "point",
      "_minzoom", 10
    )), features);
  }

  @Test
  public void testInterstateMotorwayWithoutWayInfo() {
    var rel = new OsmElement.Relation(1);
    rel.setTag("type", "route");
    rel.setTag("route", "road");
    rel.setTag("network", "US:I");
    rel.setTag("ref", "90");

    FeatureCollector features = process(lineFeatureWithRelation(
      profile.preprocessOsmRelation(rel),
      Map.of(
        "highway", "motorway"
      )));

    assertFeatures(13, List.of(mapOf(
      "_layer", "transportation",
      "class", "motorway",
      "network", "us-interstate",
      "_minzoom", 4
    ), Map.of(
      "_layer", "transportation_name",
      "class", "motorway",
      "ref", "90",
      "ref_length", 2,
      "network", "us-interstate",
      "brunnel", "<null>",
      "route_1", "US:I=90",
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
  public void testBridgeConstruction() {
    assertFeatures(14, List.of(), process(lineFeature(Map.of(
      "highway", "construction",
      "construction", "bridge",
      "man_made", "bridge",
      "layer", "1"
    ))));
    assertFeatures(14, List.of(Map.of(
      "_layer", "transportation",
      "class", "minor_construction",
      "brunnel", "bridge",
      "layer", 1L
    )), process(closedWayFeature(Map.of(
      "highway", "construction",
      "construction", "bridge",
      "man_made", "bridge",
      "layer", "1"
    ))));
  }

  @Test
  public void testIgnoreManMadeWhenNotBridgeOrPier() {
    // https://github.com/onthegomap/planetiler/issues/69
    assertFeatures(14, List.of(), process(lineFeature(Map.of(
      "man_made", "storage_tank",
      "service", "driveway"
    ))));
    assertFeatures(14, List.of(), process(lineFeature(Map.of(
      "man_made", "courtyard",
      "service", "driveway"
    ))));
    assertFeatures(14, List.of(), process(lineFeature(Map.of(
      "man_made", "courtyard",
      "service", "driveway",
      "name", "Named Driveway"
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
      "_minzoom", 14
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
  public void testNamedTrack() {
    assertFeatures(13, List.of(Map.of(
      "_layer", "transportation",
      "class", "track",
      "surface", "unpaved",
      "horse", "yes",
      "_minzoom", 13
    ), Map.of(
      "_layer", "transportation_name",
      "class", "track",
      "name", "name",
      "_minzoom", 13
    )), process(lineFeature(Map.of(
      "highway", "track",
      "surface", "dirt",
      "horse", "yes",
      "name", "name"
    ))));
  }

  @Test
  public void testUnnamedTrack() {
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

  @Test
  public void testBusway() {
    assertFeatures(13, List.of(Map.of(
      "_layer", "transportation",
      "class", "busway",
      "brunnel", "tunnel",
      "_minzoom", 11
    ), Map.of(
      "_layer", "transportation_name",
      "class", "busway",
      "name", "Silver Line",
      "_minzoom", 12
    )), process(lineFeature(Map.of(
      "access", "no",
      "bus", "yes",
      "highway", "busway",
      "layer", "-1",
      "name", "Silver Line",
      "trolley_wire", "yes",
      "tunnel", "yes"
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
      "oneway", "<null>",
      "ramp", "<null>",
      "network", "us-highway",
      "_minzoom", 7
    ), Map.of(
      "_layer", "transportation_name",
      "class", "primary",
      "name", "Memorial Drive",
      "name_en", "Memorial Drive",
      "ref", "3",
      "ref_length", 1,
      "network", "us-highway",
      "route_1", "US:US=3",
      "route_2", "US:MA=2",
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
      "route_1", "US:US=3",
      "route_2", "US:MA=2",
      "ref", "3",
      "network", "us-highway"
    )), process(lineFeatureWithRelation(
      Stream.concat(
        profile.preprocessOsmRelation(relMA).stream(),
        Stream.concat( // ignore duplicates
          profile.preprocessOsmRelation(relUS).stream(),
          profile.preprocessOsmRelation(relUS).stream()
        )
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
      "class", "primary",
      "network", "<null>"
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
      "ramp", "<null>",
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
      "ramp", "<null>",
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
      "ramp", "<null>",
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
      "oneway", "<null>",
      "ramp", "<null>",

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
      "oneway", "<null>",
      "ramp", "<null>",

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
      "layer", "<null>",
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
    assertFeatures(12, List.of(Map.of(
      "_layer", "transportation",
      "class", "aerialway",
      "subclass", "gondola",

      "_minzoom", 12,
      "_maxzoom", 14,
      "_type", "line"
    ), Map.of(
      "_layer", "transportation_name",
      "class", "aerialway",
      "subclass", "gondola",
      "name", "Summit Gondola",

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
    ), Map.of(
      "_layer", "transportation_name",
      "class", "ferry",
      "name", "Boston - Provincetown Ferry",

      "_minzoom", 12,
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
    Map<String, Object> pedestrianArea = Map.of(
      "_layer", "transportation",
      "class", "path",
      "subclass", "pedestrian",

      "_minzoom", 13,
      "_maxzoom", 14,
      "_type", "polygon"
    );
    Map<String, Object> circularPath = Map.of(
      "_layer", "transportation",
      "class", "path",
      "subclass", "pedestrian",

      "_minzoom", 14,
      "_maxzoom", 14,
      "_type", "line"
    );
    assertFeatures(14, List.of(pedestrianArea), process(closedWayFeature(Map.of(
      "highway", "pedestrian",
      "area", "yes",
      "foot", "yes"
    ))));
    assertFeatures(14, List.of(pedestrianArea), process(polygonFeature(Map.of(
      "highway", "pedestrian",
      "foot", "yes"
    ))));
    assertFeatures(14, List.of(circularPath), process(closedWayFeature(Map.of(
      "highway", "pedestrian",
      "foot", "yes"
    ))));
    assertFeatures(14, List.of(circularPath), process(closedWayFeature(Map.of(
      "highway", "pedestrian",
      "foot", "yes",
      "area", "no"
    ))));
    // ignore underground pedestrian areas
    assertFeatures(14, List.of(),
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

  @Test
  public void testTransportationNameLayerRequiresTransportationLayer() {
    var profile = new BasemapProfile(translations, PlanetilerConfig.from(Arguments.of(
      "only_layers", "transportation_name"
    )), Stats.inMemory());
    SourceFeature feature = lineFeature(Map.of(
      "highway", "path",
      "name", "test"
    ));
    var collector = featureCollectorFactory.get(feature);
    profile.processFeature(feature, collector);
    assertFeatures(14, List.of(Map.of(
      "_layer", "transportation_name",
      "class", "path",
      "name", "test"
    ), Map.of(
      "_layer", "transportation",
      "class", "path"
    )), collector);
  }
}
