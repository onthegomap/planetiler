package com.onthegomap.flatmap.openmaptiles.layers;

import static com.onthegomap.flatmap.TestUtils.assertSubmap;
import static com.onthegomap.flatmap.TestUtils.newLineString;
import static com.onthegomap.flatmap.TestUtils.newPoint;
import static com.onthegomap.flatmap.TestUtils.rectangle;
import static com.onthegomap.flatmap.openmaptiles.OpenMapTilesProfile.OSM_SOURCE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import com.onthegomap.flatmap.Arguments;
import com.onthegomap.flatmap.CommonParams;
import com.onthegomap.flatmap.FeatureCollector;
import com.onthegomap.flatmap.SourceFeature;
import com.onthegomap.flatmap.TestUtils;
import com.onthegomap.flatmap.Translations;
import com.onthegomap.flatmap.VectorTileEncoder;
import com.onthegomap.flatmap.Wikidata;
import com.onthegomap.flatmap.geo.GeoUtils;
import com.onthegomap.flatmap.monitoring.Stats;
import com.onthegomap.flatmap.openmaptiles.OpenMapTilesProfile;
import com.onthegomap.flatmap.read.ReaderFeature;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.StreamSupport;

public abstract class BaseLayerTest {

  final Wikidata.WikidataTranslations wikidataTranslations = new Wikidata.WikidataTranslations();
  final Translations translations = Translations.defaultProvider(List.of("en", "es", "de"))
    .addTranslationProvider(wikidataTranslations);

  final CommonParams params = CommonParams.defaults();
  final OpenMapTilesProfile profile = new OpenMapTilesProfile(translations, Arguments.of(),
    new Stats.InMemory());
  final Stats stats = new Stats.InMemory();
  final FeatureCollector.Factory featureCollectorFactory = new FeatureCollector.Factory(params, stats);

  static void assertFeatures(int zoom, List<Map<String, Object>> expected, Iterable<FeatureCollector.Feature> actual) {
    List<FeatureCollector.Feature> actualList = StreamSupport.stream(actual.spliterator(), false).toList();
    assertEquals(expected.size(), actualList.size(), "size");
    for (int i = 0; i < expected.size(); i++) {
      assertSubmap(expected.get(i), TestUtils.toMap(actualList.get(i), zoom));
    }
  }

  VectorTileEncoder.Feature pointFeature(String layer, Map<String, Object> map, int group) {
    return new VectorTileEncoder.Feature(
      layer,
      1,
      VectorTileEncoder.encodeGeometry(newPoint(0, 0)),
      new HashMap<>(map),
      group
    );
  }

  FeatureCollector process(SourceFeature feature) {
    var collector = featureCollectorFactory.get(feature);
    profile.processFeature(feature, collector);
    return collector;
  }

  void assertCoversZoomRange(int minzoom, int maxzoom, String layer, FeatureCollector... featureCollectors) {
    Map<?, ?>[] zooms = new Map[Math.max(15, maxzoom + 1)];
    for (var features : featureCollectors) {
      for (var feature : features) {
        if (feature.getLayer().equals(layer)) {
          for (int zoom = feature.getMinZoom(); zoom <= feature.getMaxZoom(); zoom++) {
            Map<String, Object> map = TestUtils.toMap(feature, zoom);
            if (zooms[zoom] != null) {
              fail("Multiple features at z" + zoom + ":\n" + zooms[zoom] + "\n" + map);
            }
            zooms[zoom] = map;
          }
        }
      }
    }
    for (int zoom = 0; zoom <= 14; zoom++) {
      if (zoom < minzoom || zoom > maxzoom) {
        if (zooms[zoom] != null) {
          fail("Expected nothing at z" + zoom + " but found: " + zooms[zoom]);
        }
      } else {
        if (zooms[zoom] == null) {
          fail("No feature at z" + zoom);
        }
      }
    }
  }

  SourceFeature pointFeature(Map<String, Object> props) {
    return new ReaderFeature(
      newPoint(0, 0),
      new HashMap<>(props),
      OSM_SOURCE,
      null,
      0
    );
  }

  SourceFeature lineFeature(Map<String, Object> props) {
    return new ReaderFeature(
      newLineString(0, 0, 1, 1),
      new HashMap<>(props),
      OSM_SOURCE,
      null,
      0
    );
  }

  SourceFeature polygonFeatureWithArea(double area, Map<String, Object> props) {
    return new ReaderFeature(
      GeoUtils.worldToLatLonCoords(rectangle(0, Math.sqrt(area))),
      new HashMap<>(props),
      OSM_SOURCE,
      null,
      0
    );
  }

  SourceFeature polygonFeature(Map<String, Object> props) {
    return polygonFeatureWithArea(1, props);
  }
}
