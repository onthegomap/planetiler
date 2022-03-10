package com.onthegomap.planetiler.examples;

import static com.onthegomap.planetiler.TestUtils.assertContains;
import static com.onthegomap.planetiler.TestUtils.newLineString;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.onthegomap.planetiler.FeatureCollector;
import com.onthegomap.planetiler.TestUtils;
import com.onthegomap.planetiler.VectorTile;
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

public class BikeRouteOverlayTest {

  private final BikeRouteOverlay profile = new BikeRouteOverlay();

  @Test
  public void testSourceFeatureProcessing() {
    // step 1) preprocess an example OSM relation
    var relationResult = profile.preprocessOsmRelation(new OsmElement.Relation(1, Map.of(
      "type", "route",
      "route", "bicycle",
      "name", "rail trail",
      "network", "lcn",
      "ref", "1"
    ), List.of(
      new OsmElement.Relation.Member(OsmElement.Type.WAY, 2, "role")
    )));

    // step 2) process a way contained in that relation
    var way = SimpleFeature.createFakeOsmFeature(TestUtils.newLineString(
      10, 20, // point 1: 10 east 20 north
      30, 40 // point 2: 30 east 40 north
    ), Map.of(), null, null, 2,
      relationResult.stream().map(info -> new OsmReader.RelationMember<>("role", info)).toList());
    List<FeatureCollector.Feature> mapFeatures = TestUtils.processSourceFeature(way, profile);

    // verify output geometry
    assertEquals(1, mapFeatures.size());
    var feature = mapFeatures.get(0);
    assertEquals("bicycle-route-local", feature.getLayer());
    assertEquals(Map.of(
      "name", "rail trail",
      "ref", "1"
    ), feature.getAttrsAtZoom(14));
    // output geometry is in world coordinates where 0,0 is top left and 1,1 is bottom right
    assertEquals(0.085, feature.getGeometry().getLength(), 1e-2);
    assertEquals(0, feature.getMinZoom());
    assertEquals(14, feature.getMaxZoom());
  }

  @Test
  public void testTilePostProcessingMergesConnectedLines() {
    String layer = "bicycle-route-local";
    Map<String, Object> attrs = Map.of(
      "name", "rail trail",
      "ref", "1"
    );
    // segment 1: (0, 0) to (10, 0)
    var line1 = new VectorTile.Feature(layer, 1, // id
      VectorTile.encodeGeometry(newLineString(0, 0, 10, 0)),
      attrs
    );
    // segment 2: (10, 0) to (20, 0)
    var line2 = new VectorTile.Feature(layer, 2, // id
      VectorTile.encodeGeometry(newLineString(10, 0, 20, 0)),
      attrs
    );
    // merged: (0, 0) to (20, 0)
    var connected = new VectorTile.Feature(layer, 1, // id
      VectorTile.encodeGeometry(newLineString(0, 0, 20, 0)),
      attrs
    );

    // ensure that 2 touching linestrings with same tags are merged
    assertEquals(
      List.of(connected),
      profile.postProcessLayerFeatures(layer, 14, List.of(line1, line2))
    );
  }

  @Test
  public void integrationTest(@TempDir Path tmpDir) throws Exception {
    Path dbPath = tmpDir.resolve("output.mbtiles");
    BikeRouteOverlay.run(Arguments.of(
      // Override input source locations
      "osm_path", TestUtils.pathToResource("monaco-latest.osm.pbf"),
      // Override temp dir location
      "tmp", tmpDir.toString(),
      // Override output location
      "mbtiles", dbPath.toString()
    ));
    try (Mbtiles mbtiles = Mbtiles.newReadOnlyDatabase(dbPath)) {
      Map<String, String> metadata = mbtiles.metadata().getAll();
      assertEquals("Bike Paths Overlay", metadata.get("name"));
      assertContains("openstreetmap.org/copyright", metadata.get("attribution"));

      TestUtils
        .assertNumFeatures(mbtiles, "bicycle-route-international", 14, Map.of(
          "name", "EuroVelo 8 - Mediterranean Route - part Monaco",
          "ref", "EV8"
        ), GeoUtils.WORLD_LAT_LON_BOUNDS, 25, LineString.class);
    }
  }
}
