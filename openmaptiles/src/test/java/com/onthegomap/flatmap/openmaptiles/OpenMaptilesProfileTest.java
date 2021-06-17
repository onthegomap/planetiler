package com.onthegomap.flatmap.openmaptiles;

import static com.onthegomap.flatmap.TestUtils.assertSubmap;
import static com.onthegomap.flatmap.TestUtils.newLineString;
import static com.onthegomap.flatmap.TestUtils.newPoint;
import static com.onthegomap.flatmap.openmaptiles.OpenMapTilesProfile.OSM_SOURCE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.DynamicTest.dynamicTest;

import com.onthegomap.flatmap.Arguments;
import com.onthegomap.flatmap.CommonParams;
import com.onthegomap.flatmap.FeatureCollector;
import com.onthegomap.flatmap.SourceFeature;
import com.onthegomap.flatmap.TestUtils;
import com.onthegomap.flatmap.Translations;
import com.onthegomap.flatmap.VectorTileEncoder;
import com.onthegomap.flatmap.Wikidata;
import com.onthegomap.flatmap.geo.GeometryException;
import com.onthegomap.flatmap.monitoring.Stats;
import com.onthegomap.flatmap.openmaptiles.layers.MountainPeak;
import com.onthegomap.flatmap.read.ReaderFeature;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.StreamSupport;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;

public class OpenMaptilesProfileTest {

  private final Translations translations = Translations.defaultProvider(List.of("en", "es", "de"));
  private final Wikidata.WikidataTranslations wikidataTranslations = new Wikidata.WikidataTranslations();

  {
    translations.addTranslationProvider(wikidataTranslations);
  }

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
  public void testMountainPeakPostProcessingEmpty() throws GeometryException {
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
}
