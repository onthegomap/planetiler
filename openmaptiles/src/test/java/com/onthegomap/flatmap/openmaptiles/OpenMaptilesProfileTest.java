package com.onthegomap.flatmap.openmaptiles;

import static com.onthegomap.flatmap.TestUtils.assertSubmap;
import static com.onthegomap.flatmap.TestUtils.newLineString;
import static com.onthegomap.flatmap.TestUtils.newPoint;
import static com.onthegomap.flatmap.openmaptiles.OpenMapTilesProfile.OSM_SOURCE;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.onthegomap.flatmap.Arguments;
import com.onthegomap.flatmap.CommonParams;
import com.onthegomap.flatmap.FeatureCollector;
import com.onthegomap.flatmap.SourceFeature;
import com.onthegomap.flatmap.TestUtils;
import com.onthegomap.flatmap.Translations;
import com.onthegomap.flatmap.Wikidata;
import com.onthegomap.flatmap.monitoring.Stats;
import com.onthegomap.flatmap.read.ReaderFeature;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.StreamSupport;
import org.junit.jupiter.api.Test;

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

  @Test
  public void testMountainPeak() {
    var peak = process(pointFeature(Map.of(
      "natural", "peak",
      "name", "test",
      "ele", "100"
    )));
    assertFeatures(14, List.of(Map.of(
      "name", "test",
      "class", "peak",
      "ele", 100,
      "ele_ft", 328,

      "_layer", "mountain_peak",
      "_type", "point",
      "_minzoom", 7,
      "_maxzoom", 14,
      "_buffer", 64d,
      "_labelgrid_limit", 0
    )), peak);
    assertFeatures(13, List.of(Map.of(
      "_labelgrid_limit", 5,
      "_labelgrid_size", 100d
    )), peak);

    assertFeatures(14, List.of(Map.of(
      "class", "volcano"
    )), process(pointFeature(Map.of(
      "natural", "volcano",
      "ele", "100"
    ))));

    // no elevation
    assertFeatures(14, List.of(), process(pointFeature(Map.of(
      "natural", "volcano"
    ))));
    // bogus elevation
    assertFeatures(14, List.of(), process(pointFeature(Map.of(
      "natural", "volcano",
      "ele", "11000"
    ))));
  }

  @Test
  public void testMountainPeakNames() {
    assertFeatures(14, List.of(Map.of(
      "name", "name",
      "name_en", "english name",
      "name_de", "german name"
    )), process(pointFeature(Map.of(
      "natural", "peak",
      "name", "name",
      "name:en", "english name",
      "name:de", "german name",
      "ele", "100"
    ))));
    assertFeatures(14, List.of(Map.of(
      "name", "name",
      "name_en", "name",
      "name_de", "german name"
    )), process(pointFeature(Map.of(
      "natural", "peak",
      "name", "name",
      "name:de", "german name",
      "ele", "100"
    ))));
    assertFeatures(14, List.of(Map.of(
      "name", "name",
      "name_en", "name",
      "name_de", "name"
    )), process(pointFeature(Map.of(
      "natural", "peak",
      "name", "name",
      "ele", "100"
    ))));
  }

  @Test
  public void testMountainPeakNameTranslations() {
    wikidataTranslations.put(123, "en", "wikidata en");
    wikidataTranslations.put(123, "es", "wikidata es");
    wikidataTranslations.put(123, "de", "wikidata de");
    var feature = process(pointFeature(Map.of(
      "natural", "peak",
      "name", "name",
      "name:en", "english name",
      "name:de", "german name",
      "wikidata", "Q123",
      "ele", "100"
    )));
    assertFeatures(14, List.of(Map.of(
      "name", "name",
      "name_en", "english name",
      "name_de", "german name",
      "name:es", "wikidata es"
    )), feature);
  }

  @Test
  public void testMountainPeakMustBePoint() {
    assertFeatures(14, List.of(), process(lineFeature(Map.of(
      "natural", "peak",
      "name", "name",
      "ele", "100"
    ))));
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
