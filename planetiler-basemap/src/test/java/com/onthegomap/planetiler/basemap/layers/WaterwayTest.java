package com.onthegomap.planetiler.basemap.layers;

import static com.onthegomap.planetiler.TestUtils.newLineString;
import static com.onthegomap.planetiler.basemap.BasemapProfile.NATURAL_EARTH_SOURCE;
import static com.onthegomap.planetiler.basemap.BasemapProfile.OSM_SOURCE;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.onthegomap.planetiler.FeatureCollector;
import com.onthegomap.planetiler.VectorTile;
import com.onthegomap.planetiler.geo.GeometryException;
import com.onthegomap.planetiler.reader.SimpleFeature;
import com.onthegomap.planetiler.reader.osm.OsmElement;
import com.onthegomap.planetiler.reader.osm.OsmReader;
import com.onthegomap.planetiler.reader.osm.OsmRelationInfo;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class WaterwayTest extends AbstractLayerTest {

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void testOsmWaterwayRelation(boolean isLongEnough) throws GeometryException {
    var rel = new OsmElement.Relation(1);
    rel.setTag("name", "River Relation");
    rel.setTag("name:es", "ES name");
    rel.setTag("waterway", "river");

    List<OsmRelationInfo> relationInfos = profile.preprocessOsmRelation(rel);
    FeatureCollector features = process(SimpleFeature.createFakeOsmFeature(
      newLineString(0, 0, 0, isLongEnough ? 3 : 1),
      Map.of(),
      OSM_SOURCE,
      null,
      0,
      (relationInfos == null ? List.<OsmRelationInfo>of() : relationInfos).stream()
        .map(r -> new OsmReader.RelationMember<>("", r)).toList()
    ));
    assertFeatures(14, List.of(Map.of(
      "class", "river",
      "name", "River Relation",
      "name:es", "ES name",
      "_relid", 1L,

      "_layer", "waterway",
      "_type", "line",
      "_minzoom", 6,
      "_maxzoom", 8,
      "_buffer", 4d
    )), features);

    // ensure that post-processing combines waterways, and filters out ones that
    // belong to rivers that are not long enough to be shown
    var line1 = new VectorTile.Feature(
      Waterway.LAYER_NAME,
      1,
      VectorTile.encodeGeometry(newLineString(0, 0, 10, 0)),
      mapOf("name", "river", "_relid", 1L),
      0
    );
    var line2 = new VectorTile.Feature(
      Waterway.LAYER_NAME,
      1,
      VectorTile.encodeGeometry(newLineString(10, 0, 20, 0)),
      mapOf("name", "river", "_relid", 1L),
      0
    );
    var connected = new VectorTile.Feature(
      Waterway.LAYER_NAME,
      1,
      VectorTile.encodeGeometry(newLineString(0, 0, 20, 0)),
      mapOf("name", "river"),
      0
    );

    assertEquals(
      isLongEnough ? List.of(connected) : List.of(),
      profile.postProcessLayerFeatures(Waterway.LAYER_NAME, 8, new ArrayList<>(List.of(line1, line2)))
    );
  }

  @Test
  public void testWaterwayImportantRiverProcess() {
    var charlesRiver = process(lineFeature(Map.of(
      "waterway", "river",
      "name", "charles river",
      "name:es", "es name"
    )));
    assertFeatures(14, List.of(Map.of(
      "class", "river",
      "name", "charles river",
      "name:es", "es name",
      "intermittent", 0,

      "_layer", "waterway",
      "_type", "line",
      "_minzoom", 9,
      "_maxzoom", 14,
      "_buffer", 4d
    )), charlesRiver);
    assertFeatures(11, List.of(Map.of(
      "class", "river",
      "name", "charles river",
      "name:es", "es name",
      "intermittent", "<null>",
      "_buffer", 13.082664546679323
    )), charlesRiver);
    assertFeatures(10, List.of(Map.of(
      "class", "river",
      "_buffer", 26.165329093358647
    )), charlesRiver);
    assertFeatures(9, List.of(Map.of(
      "class", "river",
      "_buffer", 26.165329093358647
    )), charlesRiver);
  }

  @Test
  public void testWaterwayImportantRiverPostProcess() throws GeometryException {
    var line1 = new VectorTile.Feature(
      Waterway.LAYER_NAME,
      1,
      VectorTile.encodeGeometry(newLineString(0, 0, 10, 0)),
      Map.of("name", "river"),
      0
    );
    var line2 = new VectorTile.Feature(
      Waterway.LAYER_NAME,
      1,
      VectorTile.encodeGeometry(newLineString(10, 0, 20, 0)),
      Map.of("name", "river"),
      0
    );
    var connected = new VectorTile.Feature(
      Waterway.LAYER_NAME,
      1,
      VectorTile.encodeGeometry(newLineString(0, 0, 20, 0)),
      Map.of("name", "river"),
      0
    );

    assertEquals(
      List.of(),
      profile.postProcessLayerFeatures(Waterway.LAYER_NAME, 11, List.of())
    );
    assertEquals(
      List.of(line1, line2),
      profile.postProcessLayerFeatures(Waterway.LAYER_NAME, 12, List.of(line1, line2))
    );
    assertEquals(
      List.of(connected),
      profile.postProcessLayerFeatures(Waterway.LAYER_NAME, 11, List.of(line1, line2))
    );
  }

  @Test
  public void testWaterwaySmaller() {
    // river with no name is not important
    assertFeatures(14, List.of(Map.of(
      "class", "river",
      "brunnel", "bridge",

      "_layer", "waterway",
      "_type", "line",
      "_minzoom", 12
    )), process(lineFeature(Map.of(
      "waterway", "river",
      "bridge", "1"
    ))));

    assertFeatures(14, List.of(Map.of(
      "class", "canal",
      "_layer", "waterway",
      "_type", "line",
      "_minzoom", 12
    )), process(lineFeature(Map.of(
      "waterway", "canal",
      "name", "name"
    ))));

    assertFeatures(14, List.of(Map.of(
      "class", "stream",
      "_layer", "waterway",
      "_type", "line",
      "_minzoom", 13
    )), process(lineFeature(Map.of(
      "waterway", "stream",
      "name", "name"
    ))));
  }

  @Test
  public void testWaterwayNaturalEarth() {
    assertFeatures(3, List.of(Map.of(
      "class", "river",
      "name", "<null>",
      "intermittent", "<null>",

      "_layer", "waterway",
      "_type", "line",
      "_minzoom", 3,
      "_maxzoom", 3
    )), process(SimpleFeature.create(
      newLineString(0, 0, 1, 1),
      Map.of(
        "featurecla", "River",
        "name", "name"
      ),
      NATURAL_EARTH_SOURCE,
      "ne_110m_rivers_lake_centerlines",
      0
    )));

    assertFeatures(6, List.of(Map.of(
      "class", "river",
      "intermittent", "<null>",

      "_layer", "waterway",
      "_type", "line",
      "_minzoom", 4,
      "_maxzoom", 5
    )), process(SimpleFeature.create(
      newLineString(0, 0, 1, 1),
      Map.of(
        "featurecla", "River",
        "name", "name"
      ),
      NATURAL_EARTH_SOURCE,
      "ne_50m_rivers_lake_centerlines",
      0
    )));
  }
}
