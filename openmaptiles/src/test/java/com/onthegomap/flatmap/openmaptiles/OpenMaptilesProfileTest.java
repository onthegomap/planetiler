package com.onthegomap.flatmap.openmaptiles;

import static com.onthegomap.flatmap.TestUtils.assertSubmap;
import static com.onthegomap.flatmap.TestUtils.newLineString;
import static com.onthegomap.flatmap.TestUtils.newPoint;
import static com.onthegomap.flatmap.TestUtils.rectangle;
import static com.onthegomap.flatmap.openmaptiles.OpenMapTilesProfile.LAKE_CENTERLINE_SOURCE;
import static com.onthegomap.flatmap.openmaptiles.OpenMapTilesProfile.NATURAL_EARTH_SOURCE;
import static com.onthegomap.flatmap.openmaptiles.OpenMapTilesProfile.OSM_SOURCE;
import static com.onthegomap.flatmap.openmaptiles.OpenMapTilesProfile.WATER_POLYGON_SOURCE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.DynamicTest.dynamicTest;

import com.graphhopper.reader.ReaderNode;
import com.onthegomap.flatmap.Arguments;
import com.onthegomap.flatmap.CommonParams;
import com.onthegomap.flatmap.FeatureCollector;
import com.onthegomap.flatmap.SourceFeature;
import com.onthegomap.flatmap.TestUtils;
import com.onthegomap.flatmap.Translations;
import com.onthegomap.flatmap.VectorTileEncoder;
import com.onthegomap.flatmap.Wikidata;
import com.onthegomap.flatmap.geo.GeoUtils;
import com.onthegomap.flatmap.geo.GeometryException;
import com.onthegomap.flatmap.monitoring.Stats;
import com.onthegomap.flatmap.openmaptiles.layers.MountainPeak;
import com.onthegomap.flatmap.openmaptiles.layers.Waterway;
import com.onthegomap.flatmap.read.ReaderFeature;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.StreamSupport;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;

public class OpenMaptilesProfileTest {

  private final Wikidata.WikidataTranslations wikidataTranslations = new Wikidata.WikidataTranslations();
  private final Translations translations = Translations.defaultProvider(List.of("en", "es", "de"))
    .addTranslationProvider(wikidataTranslations);

  private final CommonParams params = CommonParams.defaults();
  private final OpenMapTilesProfile profile = new OpenMapTilesProfile(translations, Arguments.of(),
    new Stats.InMemory());
  private final Stats stats = new Stats.InMemory();
  private final FeatureCollector.Factory featureCollectorFactory = new FeatureCollector.Factory(params, stats);

  private static void assertFeatures(int zoom, List<Map<String, Object>> expected, FeatureCollector actual) {
    List<FeatureCollector.Feature> actualList = StreamSupport.stream(actual.spliterator(), false).toList();
    assertEquals(expected.size(), actualList.size(), "size");
    for (int i = 0; i < expected.size(); i++) {
      assertSubmap(expected.get(i), TestUtils.toMap(actualList.get(i), zoom));
    }
  }

  @TestFactory
  public List<DynamicTest> mountainPeakProcessing() {
    wikidataTranslations.put(123, "es", "es wd name");
    return List.of(
      dynamicTest("happy path", () -> {
        var peak = process(pointFeature(Map.of(
          "natural", "peak",
          "name", "test",
          "ele", "100",
          "wikidata", "Q123"
        )));
        assertFeatures(14, List.of(Map.of(
          "class", "peak",
          "ele", 100,
          "ele_ft", 328,

          "_layer", "mountain_peak",
          "_type", "point",
          "_minzoom", 7,
          "_maxzoom", 14,
          "_buffer", 64d
        )), peak);
        assertFeatures(14, List.of(Map.of(
          "name:latin", "test",
          "name", "test",
          "name:es", "es wd name"
        )), peak);
      }),

      dynamicTest("labelgrid", () -> {
        var peak = process(pointFeature(Map.of(
          "natural", "peak",
          "ele", "100"
        )));
        assertFeatures(14, List.of(Map.of(
          "_labelgrid_limit", 0
        )), peak);
        assertFeatures(13, List.of(Map.of(
          "_labelgrid_limit", 5,
          "_labelgrid_size", 100d
        )), peak);
      }),

      dynamicTest("volcano", () ->
        assertFeatures(14, List.of(Map.of(
          "class", "volcano"
        )), process(pointFeature(Map.of(
          "natural", "volcano",
          "ele", "100"
        ))))),

      dynamicTest("no elevation", () ->
        assertFeatures(14, List.of(), process(pointFeature(Map.of(
          "natural", "volcano"
        ))))),

      dynamicTest("bogus elevation", () ->
        assertFeatures(14, List.of(), process(pointFeature(Map.of(
          "natural", "volcano",
          "ele", "11000"
        ))))),

      dynamicTest("ignore lines", () ->
        assertFeatures(14, List.of(), process(lineFeature(Map.of(
          "natural", "peak",
          "name", "name",
          "ele", "100"
        ))))),

      dynamicTest("zorder", () -> {
        assertFeatures(14, List.of(Map.of(
          "_zorder", 100
        )), process(pointFeature(Map.of(
          "natural", "peak",
          "ele", "100"
        ))));
        assertFeatures(14, List.of(Map.of(
          "_zorder", 10100
        )), process(pointFeature(Map.of(
          "natural", "peak",
          "name", "name",
          "ele", "100"
        ))));
        assertFeatures(14, List.of(Map.of(
          "_zorder", 20100
        )), process(pointFeature(Map.of(
          "natural", "peak",
          "name", "name",
          "wikipedia", "wikilink",
          "ele", "100"
        ))));
      })
    );
  }

  @Test
  public void testMountainPeakPostProcessing() throws GeometryException {
    assertEquals(List.of(), profile.postProcessLayerFeatures(MountainPeak.LAYER_NAME, 13, List.of()));

    assertEquals(List.of(pointFeature(
      MountainPeak.LAYER_NAME,
      Map.of("rank", 1),
      1
    )), profile.postProcessLayerFeatures(MountainPeak.LAYER_NAME, 13, List.of(pointFeature(
      MountainPeak.LAYER_NAME,
      Map.of(),
      1
    ))));

    assertEquals(List.of(
      pointFeature(
        MountainPeak.LAYER_NAME,
        Map.of("rank", 2, "name", "a"),
        1
      ), pointFeature(
        MountainPeak.LAYER_NAME,
        Map.of("rank", 1, "name", "b"),
        1
      ), pointFeature(
        MountainPeak.LAYER_NAME,
        Map.of("rank", 1, "name", "c"),
        2
      )
    ), profile.postProcessLayerFeatures(MountainPeak.LAYER_NAME, 13, List.of(
      pointFeature(
        MountainPeak.LAYER_NAME,
        Map.of("name", "a"),
        1
      ),
      pointFeature(
        MountainPeak.LAYER_NAME,
        Map.of("name", "b"),
        1
      ),
      pointFeature(
        MountainPeak.LAYER_NAME,
        Map.of("name", "c"),
        2
      )
    )));
  }

  @TestFactory
  public List<DynamicTest> aerodromeLabel() {
    wikidataTranslations.put(123, "es", "es wd name");
    return List.of(
      dynamicTest("happy path point", () -> {
        assertFeatures(14, List.of(Map.of(
          "class", "international",
          "ele", 100,
          "ele_ft", 328,
          "name", "osm name",
          "name:es", "es wd name",

          "_layer", "aerodrome_label",
          "_type", "point",
          "_minzoom", 10,
          "_maxzoom", 14,
          "_buffer", 64d
        )), process(pointFeature(Map.of(
          "aeroway", "aerodrome",
          "name", "osm name",
          "wikidata", "Q123",
          "ele", "100",
          "aerodrome", "international",
          "iata", "123",
          "icao", "1234"
        ))));
      }),

      dynamicTest("international", () -> {
        assertFeatures(14, List.of(Map.of(
          "class", "international",
          "_layer", "aerodrome_label"
        )), process(pointFeature(Map.of(
          "aeroway", "aerodrome",
          "aerodrome_type", "international"
        ))));
      }),

      dynamicTest("public", () -> {
        assertFeatures(14, List.of(Map.of(
          "class", "public",
          "_layer", "aerodrome_label"
        )), process(pointFeature(Map.of(
          "aeroway", "aerodrome",
          "aerodrome_type", "public airport"
        ))));
        assertFeatures(14, List.of(Map.of(
          "class", "public",
          "_layer", "aerodrome_label"
        )), process(pointFeature(Map.of(
          "aeroway", "aerodrome",
          "aerodrome_type", "civil"
        ))));
      }),

      dynamicTest("military", () -> {
        assertFeatures(14, List.of(Map.of(
          "class", "military",
          "_layer", "aerodrome_label"
        )), process(pointFeature(Map.of(
          "aeroway", "aerodrome",
          "aerodrome_type", "military airport"
        ))));
        assertFeatures(14, List.of(Map.of(
          "class", "military",
          "_layer", "aerodrome_label"
        )), process(pointFeature(Map.of(
          "aeroway", "aerodrome",
          "military", "airfield"
        ))));
      }),

      dynamicTest("private", () -> {
        assertFeatures(14, List.of(Map.of(
          "class", "private",
          "_layer", "aerodrome_label"
        )), process(pointFeature(Map.of(
          "aeroway", "aerodrome",
          "aerodrome_type", "private"
        ))));
        assertFeatures(14, List.of(Map.of(
          "class", "private",
          "_layer", "aerodrome_label"
        )), process(pointFeature(Map.of(
          "aeroway", "aerodrome",
          "aerodrome", "private"
        ))));
      }),

      dynamicTest("other", () -> {
        assertFeatures(14, List.of(Map.of(
          "class", "other",
          "_layer", "aerodrome_label"
        )), process(pointFeature(Map.of(
          "aeroway", "aerodrome"
        ))));
      }),

      dynamicTest("ignore non-points", () -> {
        assertFeatures(14, List.of(), process(lineFeature(Map.of(
          "aeroway", "aerodrome"
        ))));
      })
    );
  }

  @Test
  public void aerowayGate() {
    assertFeatures(14, List.of(Map.of(
      "class", "gate",
      "ref", "123",

      "_layer", "aeroway",
      "_type", "point",
      "_minzoom", 14,
      "_maxzoom", 14,
      "_buffer", 4d
    )), process(pointFeature(Map.of(
      "aeroway", "gate",
      "ref", "123"
    ))));
    assertFeatures(14, List.of(), process(lineFeature(Map.of(
      "aeroway", "gate"
    ))));
    assertFeatures(14, List.of(), process(polygonFeature(Map.of(
      "aeroway", "gate"
    ))));
  }

  @Test
  public void aerowayLine() {
    assertFeatures(14, List.of(Map.of(
      "class", "runway",
      "ref", "123",

      "_layer", "aeroway",
      "_type", "line",
      "_minzoom", 10,
      "_maxzoom", 14,
      "_buffer", 4d
    )), process(lineFeature(Map.of(
      "aeroway", "runway",
      "ref", "123"
    ))));
    assertFeatures(14, List.of(), process(pointFeature(Map.of(
      "aeroway", "runway"
    ))));
  }

  @Test
  public void aerowayPolygon() {
    assertFeatures(14, List.of(Map.of(
      "class", "runway",
      "ref", "123",

      "_layer", "aeroway",
      "_type", "polygon",
      "_minzoom", 10,
      "_maxzoom", 14,
      "_buffer", 4d
    )), process(polygonFeature(Map.of(
      "aeroway", "runway",
      "ref", "123"
    ))));
    assertFeatures(14, List.of(Map.of(
      "class", "runway",
      "ref", "123",
      "_layer", "aeroway",
      "_type", "polygon"
    )), process(polygonFeature(Map.of(
      "area:aeroway", "runway",
      "ref", "123"
    ))));
    assertFeatures(14, List.of(Map.of(
      "class", "heliport",
      "ref", "123",
      "_layer", "aeroway",
      "_type", "polygon"
    )), process(polygonFeature(Map.of(
      "aeroway", "heliport",
      "ref", "123"
    ))));
    assertFeatures(14, List.of(), process(lineFeature(Map.of(
      "aeroway", "heliport"
    ))));
    assertFeatures(14, List.of(), process(pointFeature(Map.of(
      "aeroway", "heliport"
    ))));
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
    var line1 = new VectorTileEncoder.Feature(
      Waterway.LAYER_NAME,
      1,
      VectorTileEncoder.encodeGeometry(newLineString(0, 0, 10, 0)),
      Map.of("name", "river"),
      0
    );
    var line2 = new VectorTileEncoder.Feature(
      Waterway.LAYER_NAME,
      1,
      VectorTileEncoder.encodeGeometry(newLineString(10, 0, 20, 0)),
      Map.of("name", "river"),
      0
    );
    var connected = new VectorTileEncoder.Feature(
      Waterway.LAYER_NAME,
      1,
      VectorTileEncoder.encodeGeometry(newLineString(00, 0, 20, 0)),
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
    )), process(new ReaderFeature(
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
    )), process(new ReaderFeature(
      newLineString(0, 0, 1, 1),
      Map.of(
        "featurecla", "River",
        "name", "name"
      ),
      NATURAL_EARTH_SOURCE,
      "ne_50m_rivers_lake_centerlines",
      0
    )));

    assertFeatures(6, List.of(Map.of(
      "class", "river",
      "intermittent", "<null>",

      "_layer", "waterway",
      "_type", "line",
      "_minzoom", 6,
      "_maxzoom", 8
    )), process(new ReaderFeature(
      newLineString(0, 0, 1, 1),
      Map.of(
        "featurecla", "River",
        "name", "name"
      ),
      NATURAL_EARTH_SOURCE,
      "ne_10m_rivers_lake_centerlines",
      0
    )));
  }

  @Test
  public void testWaterNaturalEarth() {
    assertFeatures(0, List.of(Map.of(
      "class", "lake",
      "intermittent", "<null>",
      "_layer", "water",
      "_type", "polygon",
      "_minzoom", 0
    )), process(new ReaderFeature(
      rectangle(0, 10),
      Map.of(),
      NATURAL_EARTH_SOURCE,
      "ne_110m_lakes",
      0
    )));

    assertFeatures(0, List.of(Map.of(
      "class", "ocean",
      "intermittent", "<null>",
      "_layer", "water",
      "_type", "polygon",
      "_minzoom", 0
    )), process(new ReaderFeature(
      rectangle(0, 10),
      Map.of(),
      NATURAL_EARTH_SOURCE,
      "ne_110m_ocean",
      0
    )));

    assertFeatures(6, List.of(Map.of(
      "class", "lake",
      "_layer", "water",
      "_type", "polygon",
      "_maxzoom", 5
    )), process(new ReaderFeature(
      rectangle(0, 10),
      Map.of(),
      NATURAL_EARTH_SOURCE,
      "ne_10m_lakes",
      0
    )));

    assertFeatures(6, List.of(Map.of(
      "class", "ocean",
      "_layer", "water",
      "_type", "polygon",
      "_maxzoom", 5
    )), process(new ReaderFeature(
      rectangle(0, 10),
      Map.of(),
      NATURAL_EARTH_SOURCE,
      "ne_10m_ocean",
      0
    )));
  }

  @Test
  public void testWaterOsmWaterPolygon() {
    assertFeatures(0, List.of(Map.of(
      "class", "ocean",
      "intermittent", "<null>",
      "_layer", "water",
      "_type", "polygon",
      "_minzoom", 6,
      "_maxzoom", 14
    )), process(new ReaderFeature(
      rectangle(0, 10),
      Map.of(),
      WATER_POLYGON_SOURCE,
      null,
      0
    )));
  }

  @Test
  public void testWater() {
    assertFeatures(14, List.of(Map.of(
      "class", "lake",
      "_layer", "water",
      "_type", "polygon",
      "_minzoom", 6,
      "_maxzoom", 14
    )), process(polygonFeature(Map.of(
      "natural", "water",
      "water", "reservoir"
    ))));
    assertFeatures(14, List.of(Map.of(
      "class", "lake",

      "_layer", "water",
      "_type", "polygon",
      "_minzoom", 6,
      "_maxzoom", 14
    )), process(polygonFeature(Map.of(
      "leisure", "swimming_pool"
    ))));
    assertFeatures(14, List.of(), process(polygonFeature(Map.of(
      "natural", "bay"
    ))));
    assertFeatures(14, List.of(Map.of()), process(polygonFeature(Map.of(
      "natural", "water"
    ))));
    assertFeatures(14, List.of(), process(polygonFeature(Map.of(
      "natural", "water",
      "covered", "yes"
    ))));
    assertFeatures(14, List.of(Map.of(
      "class", "river",
      "brunnel", "bridge",
      "intermittent", 1,

      "_layer", "water",
      "_type", "polygon",
      "_minzoom", 6,
      "_maxzoom", 14
    )), process(polygonFeature(Map.of(
      "waterway", "stream",
      "bridge", "1",
      "intermittent", "1"
    ))));
    assertFeatures(11, List.of(Map.of(
      "class", "lake",
      "brunnel", "<null>",
      "intermittent", 0,

      "_layer", "water",
      "_type", "polygon",
      "_minzoom", 6,
      "_maxzoom", 14,
      "_minpixelsize", 2d
    )), process(polygonFeature(Map.of(
      "landuse", "salt_pond",
      "bridge", "1"
    ))));
  }

  @Test
  public void testWaterNamePoint() {
    assertFeatures(11, List.of(Map.of(
      "_layer", "water"
    ), Map.of(
      "class", "lake",
      "name", "waterway",
      "name:es", "waterway es",
      "intermittent", 1,

      "_layer", "water_name",
      "_type", "point",
      "_minzoom", 9,
      "_maxzoom", 14
    )), process(polygonFeatureWithArea(1, Map.of(
      "name", "waterway",
      "name:es", "waterway es",
      "natural", "water",
      "water", "pond",
      "intermittent", "1"
    ))));
    double z11area = Math.pow((GeoUtils.metersToPixelAtEquator(0, Math.sqrt(70_000)) / 256d), 2) * Math.pow(2, 20 - 11);
    assertFeatures(10, List.of(Map.of(
      "_layer", "water"
    ), Map.of(
      "_layer", "water_name",
      "_type", "point",
      "_minzoom", 11,
      "_maxzoom", 14
    )), process(polygonFeatureWithArea(z11area, Map.of(
      "name", "waterway",
      "natural", "water",
      "water", "pond"
    ))));
  }

  @Test
  public void testWaterNameLakeline() {
    assertFeatures(11, List.of(), process(new ReaderFeature(
      newLineString(0, 0, 1, 1),
      new HashMap<>(Map.<String, Object>of(
        "OSM_ID", -10
      )),
      LAKE_CENTERLINE_SOURCE,
      null,
      0
    )));
    assertFeatures(10, List.of(Map.of(
      "_layer", "water"
    ), Map.of(
      "name", "waterway",
      "name:es", "waterway es",

      "_layer", "water_name",
      "_type", "line",
      "_geom", new TestUtils.NormGeometry(GeoUtils.latLonToWorldCoords(newLineString(0, 0, 1, 1))),
      "_minzoom", 9,
      "_maxzoom", 14,
      "_minpixelsize", "waterway".length() * 6d
    )), process(new ReaderFeature(
      GeoUtils.worldToLatLonCoords(rectangle(0, Math.sqrt(1))),
      new HashMap<>(Map.<String, Object>of(
        "name", "waterway",
        "name:es", "waterway es",
        "natural", "water",
        "water", "pond"
      )),
      OSM_SOURCE,
      null,
      10
    )));
  }

  @Test
  public void testMarinePoint() {
    assertFeatures(11, List.of(), process(new ReaderFeature(
      newLineString(0, 0, 1, 1),
      new HashMap<>(Map.<String, Object>of(
        "scalerank", 10,
        "name", "pacific ocean"
      )),
      NATURAL_EARTH_SOURCE,
      "ne_10m_geography_marine_polys",
      0
    )));

    // name match - use scale rank from NE
    assertFeatures(10, List.of(Map.of(
      "name", "Pacific",
      "name:es", "Pacific es",
      "_layer", "water_name",
      "_type", "point",
      "_minzoom", 10,
      "_maxzoom", 14
    )), process(pointFeature(Map.of(
      "rank", 9,
      "name", "Pacific",
      "name:es", "Pacific es",
      "place", "sea"
    ))));

    // name match but ocean - use min zoom=0
    assertFeatures(10, List.of(Map.of(
      "_layer", "water_name",
      "_type", "point",
      "_minzoom", 0,
      "_maxzoom", 14
    )), process(pointFeature(Map.of(
      "rank", 9,
      "name", "Pacific",
      "place", "ocean"
    ))));

    // no name match - use OSM rank
    assertFeatures(10, List.of(Map.of(
      "_layer", "water_name",
      "_type", "point",
      "_minzoom", 9,
      "_maxzoom", 14
    )), process(pointFeature(Map.of(
      "rank", 9,
      "name", "Atlantic",
      "place", "sea"
    ))));

    // no rank at all, default to 8
    assertFeatures(10, List.of(Map.of(
      "_layer", "water_name",
      "_type", "point",
      "_minzoom", 8,
      "_maxzoom", 14
    )), process(pointFeature(Map.of(
      "name", "Atlantic",
      "place", "sea"
    ))));
  }

  @Test
  public void testHousenumber() {
    assertFeatures(14, List.of(Map.of(
      "_layer", "housenumber",
      "_type", "point",
      "_minzoom", 14,
      "_maxzoom", 14,
      "_buffer", 8d
    )), process(pointFeature(Map.of(
      "addr:housenumber", "10"
    ))));
    assertFeatures(15, List.of(Map.of(
      "_layer", "housenumber",
      "_type", "point",
      "_minzoom", 14,
      "_maxzoom", 14,
      "_buffer", 8d
    )), process(polygonFeature(Map.of(
      "addr:housenumber", "10"
    ))));
  }

  @Test
  public void testCaresAboutWikidata() {
    var node = new ReaderNode(1, 1, 1);
    node.setTag("aeroway", "gate");
    assertTrue(profile.caresAboutWikidataTranslation(node));

    node.setTag("aeroway", "other");
    assertFalse(profile.caresAboutWikidataTranslation(node));
  }

  private VectorTileEncoder.Feature pointFeature(String layer, Map<String, Object> map, int group) {
    return new VectorTileEncoder.Feature(
      layer,
      1,
      VectorTileEncoder.encodeGeometry(newPoint(0, 0)),
      new HashMap<>(map),
      group
    );
  }

  private FeatureCollector process(SourceFeature feature) {
    var collector = featureCollectorFactory.get(feature);
    profile.processFeature(feature, collector);
    return collector;
  }

  private SourceFeature pointFeature(Map<String, Object> props) {
    return new ReaderFeature(
      newPoint(0, 0),
      new HashMap<>(props),
      OSM_SOURCE,
      null,
      0
    );
  }

  private SourceFeature lineFeature(Map<String, Object> props) {
    return new ReaderFeature(
      newLineString(0, 0, 1, 1),
      new HashMap<>(props),
      OSM_SOURCE,
      null,
      0
    );
  }

  private SourceFeature polygonFeatureWithArea(double area, Map<String, Object> props) {
    return new ReaderFeature(
      GeoUtils.worldToLatLonCoords(rectangle(0, Math.sqrt(area))),
      new HashMap<>(props),
      OSM_SOURCE,
      null,
      0
    );
  }

  private SourceFeature polygonFeature(Map<String, Object> props) {
    return polygonFeatureWithArea(1, props);
  }
}
