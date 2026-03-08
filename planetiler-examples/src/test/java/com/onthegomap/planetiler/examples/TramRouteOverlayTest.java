package com.onthegomap.planetiler.examples;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.onthegomap.planetiler.FeatureCollector;
import com.onthegomap.planetiler.TestUtils;
import com.onthegomap.planetiler.reader.SimpleFeature;
import com.onthegomap.planetiler.reader.osm.OsmElement;
import com.onthegomap.planetiler.reader.osm.OsmReader;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class TramRouteOverlayTest {

  private final TramRouteOverlay profileStops = new TramRouteOverlay();
  private final TramRouteOverlay profileRoutes = new TramRouteOverlay();

  @Test
  void testSourceFeatureProcessing() {
    // First, test tram stops
    var node = SimpleFeature.create(
      TestUtils.newPoint(5, 4),
      Map.of("railway", "tram_stop")
    );
    // Produce a one-element list with a node containing the tag railway=tram_stop
    List<FeatureCollector.Feature> mapFeatures = TestUtils.processSourceFeature(node, profileStops);
    assertEquals(1, mapFeatures.size());
    var feature = mapFeatures.getFirst();
    assertEquals("tram_stop", feature.getLayer());
    assertEquals(11, feature.getMinZoom());

    // Second, test tram routes
    var relationResult = profileRoutes.preprocessOsmRelation(new OsmElement.Relation(1, Map.of(
      "type", "route",
      "route", "tram",
      "ref", "17",
      "name", "Tram 17",
      "network", "DB ÖBB"
    ), List.of(
      new OsmElement.Relation.Member(OsmElement.Type.WAY, 2, "role")
    )));
    // Create a super relation of the sample route relation
    var superRelationResult = profileRoutes.preprocessOsmRelation(new OsmElement.Relation(2, Map.of(
      "type", "route_master",
      "route_master", "tram",
      "ref", "18",
      "name", "Tram 17 North-South",
      "network", "DB ÖBB"
    ), List.of(
      new OsmElement.Relation.Member(OsmElement.Type.RELATION, 1, "role")
    )));
    // Process a way element in the tram route relation
    var way = SimpleFeature.createFakeOsmFeature(TestUtils.newLineString(
        10, 20, // point 1: 10 east 20 north
        30, 40 // point 2: 30 east 40 north
      ), Map.of(), null, null, 3,
      Stream.concat(
        relationResult.stream().map(info -> new OsmReader.RelationMember<>("role", info)),
        superRelationResult.stream().map(info -> new OsmReader.RelationMember<>("role", info, List.of(1L)))
      ).toList());
    mapFeatures = TestUtils.processSourceFeature(way, profileRoutes);

    // verify output geometry
    assertEquals(2, mapFeatures.size());
    var feature1 = mapFeatures.getFirst();
    assertEquals("Tram 17", feature.getLayer());
    assertEquals(Map.of(
      "ref", "17",
      "network", "ÖBB"
    ), feature.getAttrsAtZoom(14));
    var feature2 = mapFeatures.get(1);
    assertEquals("Tram 17 North-South", feature2.getLayer());
    assertEquals(Map.of(
      "ref", "18",
      "network", "ÖBB"
    ), feature2.getAttrsAtZoom(14));
    // output geometry is in world coordinates where 0,0 is top left and 1,1 is bottom right
    assertEquals(0.085, feature.getGeometry().getLength(), 1e-2);
    assertEquals(0, feature.getMinZoom());
    assertEquals(14, feature.getMaxZoom());
  }


  @Test
  void integrationTest(@TempDir Path tempDir) throws Exception {
    var profile = new TramRouteOverlay();
    //
  }
}
