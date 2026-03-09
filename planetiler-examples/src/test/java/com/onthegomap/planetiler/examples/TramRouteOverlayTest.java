package com.onthegomap.planetiler.examples;

import static com.onthegomap.planetiler.TestUtils.assertContains;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.onthegomap.planetiler.FeatureCollector;
import com.onthegomap.planetiler.TestUtils;
import com.onthegomap.planetiler.config.Arguments;
import com.onthegomap.planetiler.geo.GeoUtils;
import com.onthegomap.planetiler.mbtiles.Mbtiles;
import com.onthegomap.planetiler.reader.SimpleFeature;
import com.onthegomap.planetiler.reader.osm.OsmElement;
import com.onthegomap.planetiler.reader.osm.OsmReader;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.Point;

public class TramRouteOverlayTest {

  private final TramRouteOverlay profileStops = new TramRouteOverlay();
  private final TramRouteOverlay profileRoutes = new TramRouteOverlay();
  private final TramRouteOverlay profileWays = new TramRouteOverlay();

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
    var pointFeature = mapFeatures.getFirst();
    assertEquals("tram_stop", pointFeature.getLayer());
    assertEquals(11, pointFeature.getMinZoom());

    // Second, test tram routes
    var relationResult = profileRoutes.preprocessOsmRelation(new OsmElement.Relation(17, Map.of(
      "type", "route",
      "route", "tram",
      "ref", "17",
      "name", "Tram 17",
      "network", "BD BBÖ"
    ), List.of(
      new OsmElement.Relation.Member(OsmElement.Type.WAY, 2, "role")
    )));
    // Process a way element in the tram route relation
    // test points: (1 west 2 south), (3 east 5 north)
    var way = SimpleFeature.createFakeOsmFeature(
      TestUtils.newLineString(-1, -2, 3, 5),
      Map.of(),
      null,
      null,
      3,
      relationResult.stream().map(info -> new OsmReader.RelationMember<>("role", info)).toList()
    );
    mapFeatures.clear();
    mapFeatures = TestUtils.processSourceFeature(way, profileRoutes);

    // verify output geometry
    assertEquals(1, mapFeatures.size());
    var routeFeature = mapFeatures.getFirst();
    assertEquals("Tram 17", routeFeature.getLayer());
    assertEquals(Map.of(
      "ref", "17",
      "network", "BD BBÖ",
      "name", "Tram 17"
    ), routeFeature.getAttrsAtZoom(10));
    // output geometry is in world coordinates where 0,0 is top left and 1,1 is bottom right
    assertEquals(0.022, routeFeature.getGeometry().getLength(), 1e-2);
    assertEquals(0, routeFeature.getMinZoom());
    assertEquals(14, routeFeature.getMaxZoom());

    // Third, test tram ways that are not in a tram route
    var tramWay = SimpleFeature.create(
      TestUtils.newLineString(8, 9, 10, 11),
      Map.of("railway", "tram")
    );
    // Produce a one-element list with a way containing the tag railway=tram
    mapFeatures.clear();
    mapFeatures = TestUtils.processSourceFeature(tramWay, profileWays);
    assertEquals(1, mapFeatures.size());
    var wayFeature = mapFeatures.getFirst();
    assertEquals("tram", wayFeature.getLayer());
  }

  @Test
  void integrationTest(@TempDir Path tmpDir) throws Exception {
    Path dbPath = tmpDir.resolve("output.mbtiles");
    TramRouteOverlay.run(Arguments.of(
      "osm_path", TestUtils.pathToResource("bremen.osm.pbf"),
      "tmpDir", tmpDir.toString(),
      "output", dbPath.toString()
    ));
    try (Mbtiles mbtiles = Mbtiles.newReadOnlyDatabase(dbPath)) {
      Map<String, String> metadata = mbtiles.metadata().toMap();
      assertEquals("Tram Routes and Stops Overlay", metadata.get("name"));
      assertContains("openstreetmap.org/copyright", metadata.get("attribution"));

      TestUtils.assertNumFeatures(
        mbtiles,
        "tram_stop",
        12,
        Map.of(),
        GeoUtils.WORLD_LAT_LON_BOUNDS,
        352,
        Point.class
      );

      TestUtils.assertNumFeatures(
        mbtiles,
        "Tram 2: Gröpelingen => Sebaldsbrück",
        12,
        Map.of(),
        GeoUtils.WORLD_LAT_LON_BOUNDS,
        39,
        LineString.class
      );

      TestUtils.assertTileDuplicates(mbtiles, 0);

      // Include a test section for tram ways
    }
  }
}
