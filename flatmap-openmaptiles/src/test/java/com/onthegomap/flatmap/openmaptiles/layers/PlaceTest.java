package com.onthegomap.flatmap.openmaptiles.layers;

import static com.onthegomap.flatmap.TestUtils.newPoint;
import static com.onthegomap.flatmap.TestUtils.rectangle;
import static com.onthegomap.flatmap.collection.FeatureGroup.SORT_KEY_MAX;
import static com.onthegomap.flatmap.collection.FeatureGroup.SORT_KEY_MIN;
import static com.onthegomap.flatmap.openmaptiles.OpenMapTilesProfile.NATURAL_EARTH_SOURCE;
import static com.onthegomap.flatmap.openmaptiles.OpenMapTilesProfile.OSM_SOURCE;
import static com.onthegomap.flatmap.openmaptiles.layers.Place.getSortKey;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import com.onthegomap.flatmap.geo.GeoUtils;
import com.onthegomap.flatmap.geo.GeometryException;
import com.onthegomap.flatmap.reader.SimpleFeature;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

public class PlaceTest extends AbstractLayerTest {

  @Test
  public void testContinent() {
    wikidataTranslations.put(49, "es", "América del Norte y América Central");
    assertFeatures(0, List.of(Map.of(
      "_layer", "place",
      "class", "continent",
      "name", "North America",
      "name:en", "North America",
      "name:es", "América del Norte y América Central",
      "name:latin", "North America",
      "rank", 1,

      "_type", "point",
      "_minzoom", 0,
      "_maxzoom", 3
    )), process(pointFeature(Map.of(
      "place", "continent",
      "wikidata", "Q49",
      "name:es", "América del Norte",
      "name", "North America",
      "name:en", "North America"
    ))));
  }

  @Test
  public void testCountry() {
    wikidataTranslations.put(30, "es", "Estados Unidos");
    process(SimpleFeature.create(
      rectangle(0, 0.25),
      Map.of(
        "name", "United States",
        "scalerank", 0,
        "labelrank", 2
      ),
      NATURAL_EARTH_SOURCE,
      "ne_10m_admin_0_countries",
      0
    ));
    assertFeatures(0, List.of(Map.of(
      "_layer", "place",
      "class", "country",
      "name", "United States of America",
      "name_en", "United States of America",
      "name:es", "Estados Unidos",
      "name:latin", "United States of America",
      "iso_a2", "US",
      "rank", 6,

      "_type", "point",
      "_minzoom", 5
    )), process(SimpleFeature.create(
      newPoint(0.5, 0.5),
      Map.of(
        "place", "country",
        "wikidata", "Q30",
        "name:es", "Estados Unidos de América",
        "name", "United States of America",
        "name:en", "United States of America",
        "country_code_iso3166_1_alpha_2", "US"
      ),
      OSM_SOURCE,
      null,
      0
    )));
    assertFeatures(0, List.of(Map.of(
      "_layer", "place",
      "class", "country",
      "name", "United States of America",
      "name_en", "United States of America",
      "name:es", "Estados Unidos",
      "name:latin", "United States of America",
      "iso_a2", "US",
      "rank", 1,

      "_type", "point",
      "_minzoom", 0
    )), process(SimpleFeature.create(
      newPoint(0.1, 0.1),
      Map.of(
        "place", "country",
        "wikidata", "Q30",
        "name:es", "Estados Unidos de América",
        "name", "United States of America",
        "name:en", "United States of America",
        "country_code_iso3166_1_alpha_2", "US"
      ),
      OSM_SOURCE,
      null,
      0
    )));
  }

  @Test
  public void testState() {
    wikidataTranslations.put(771, "es", "Massachusetts es");
    process(SimpleFeature.create(
      rectangle(0, 0.25),
      Map.of(
        "name", "Massachusetts",
        "scalerank", 0,
        "labelrank", 2,
        "datarank", 1
      ),
      NATURAL_EARTH_SOURCE,
      "ne_10m_admin_1_states_provinces",
      0
    ));

    process(SimpleFeature.create(
      rectangle(0.4, 0.6),
      Map.of(
        "name", "Massachusetts - not important",
        "scalerank", 4,
        "labelrank", 4,
        "datarank", 1
      ),
      NATURAL_EARTH_SOURCE,
      "ne_10m_admin_1_states_provinces",
      0
    ));

    // no match
    assertFeatures(0, List.of(), process(SimpleFeature.create(
      newPoint(1, 1),
      Map.of(
        "place", "state",
        "wikidata", "Q771",
        "name", "Massachusetts",
        "name:en", "Massachusetts"
      ),
      OSM_SOURCE,
      null,
      0
    )));

    // unimportant match
    assertFeatures(0, List.of(), process(SimpleFeature.create(
      newPoint(0.5, 0.5),
      Map.of(
        "place", "state",
        "wikidata", "Q771",
        "name", "Massachusetts",
        "name:en", "Massachusetts"
      ),
      OSM_SOURCE,
      null,
      0
    )));

    // important match
    assertFeatures(0, List.of(Map.of(
      "_layer", "place",
      "class", "state",
      "name", "Massachusetts",
      "name_en", "Massachusetts",
      "name:es", "Massachusetts es",
      "name:latin", "Massachusetts",
      "rank", 1,

      "_type", "point",
      "_minzoom", 2
    )), process(SimpleFeature.create(
      newPoint(0.1, 0.1),
      Map.of(
        "place", "state",
        "wikidata", "Q771",
        "name", "Massachusetts",
        "name:en", "Massachusetts"
      ),
      OSM_SOURCE,
      null,
      0
    )));
  }

  @Test
  public void testIslandPoint() {
    assertFeatures(0, List.of(Map.of(
      "_layer", "place",
      "class", "island",
      "name", "Nantucket",
      "name_en", "Nantucket",
      "name:latin", "Nantucket",
      "rank", 7,

      "_type", "point",
      "_minzoom", 12
    )), process(pointFeature(
      Map.of(
        "place", "island",
        "name", "Nantucket",
        "name:en", "Nantucket"
      ))));
  }

  @Test
  public void testIslandPolygon() {
    assertFeatures(0, List.of(Map.of(
      "_layer", "place",
      "class", "island",
      "name", "Nantucket",
      "name_en", "Nantucket",
      "name:latin", "Nantucket",
      "rank", 3,

      "_type", "point",
      "_minzoom", 8
    )), process(polygonFeatureWithArea(1,
      Map.of(
        "place", "island",
        "name", "Nantucket",
        "name:en", "Nantucket"
      ))));

    double rank4area = Math.pow(GeoUtils.metersToPixelAtEquator(0, Math.sqrt(40_000_000 - 1)) / 256d, 2);

    assertFeatures(0, List.of(Map.of(
      "_layer", "place",
      "class", "island",
      "name", "Nantucket",
      "rank", 4,

      "_type", "point",
      "_minzoom", 9
    )), process(polygonFeatureWithArea(rank4area,
      Map.of(
        "place", "island",
        "name", "Nantucket",
        "name:en", "Nantucket"
      ))));
  }

  @Test
  public void testPlaceSortKeyRanking() {
    int[] sortKeys = new int[]{
      // max
      getSortKey(0, Place.PlaceType.CITY, 1_000_000_000, "name"),

      getSortKey(0, Place.PlaceType.CITY, 1_000_000_000, "name longer"),
      getSortKey(0, Place.PlaceType.CITY, 1_000_000_000, "x".repeat(32)),

      getSortKey(0, Place.PlaceType.CITY, 10_000_000, "name"),
      getSortKey(0, Place.PlaceType.CITY, 0, "name"),

      getSortKey(0, Place.PlaceType.TOWN, 1_000_000_000, "name"),
      getSortKey(0, Place.PlaceType.ISOLATED_DWELLING, 1_000_000_000, "name"),
      getSortKey(0, null, 1_000_000_000, "name"),

      getSortKey(1, Place.PlaceType.CITY, 1_000_000_000, "name"),
      getSortKey(10, Place.PlaceType.CITY, 1_000_000_000, "name"),
      getSortKey(null, Place.PlaceType.CITY, 1_000_000_000, "name"),

      // min
      getSortKey(null, null, 0, null),
    };
    for (int i = 0; i < sortKeys.length; i++) {
      if (sortKeys[i] < SORT_KEY_MIN) {
        fail("Item at index " + i + " is < " + SORT_KEY_MIN + ": " + sortKeys[i]);
      }
      if (sortKeys[i] > SORT_KEY_MAX) {
        fail("Item at index " + i + " is > " + SORT_KEY_MAX + ": " + sortKeys[i]);
      }
    }
    assertAscending(sortKeys);
  }

  @Test
  public void testCountryCapital() {
    process(SimpleFeature.create(
      newPoint(0, 0),
      Map.of(
        "name", "Washington, D.C.",
        "scalerank", 0,
        "wikidataid", "Q61"
      ),
      NATURAL_EARTH_SOURCE,
      "ne_10m_populated_places",
      0
    ));
    assertFeatures(7, List.of(Map.of(
      "_layer", "place",
      "class", "city",
      "name", "Washington, D.C.",
      "rank", 1,
      "capital", 2,
      "_labelgrid_limit", 0,
      "_labelgrid_size", 128d,

      "_type", "point",
      "_minzoom", 2
    )), process(pointFeature(
      Map.of(
        "place", "city",
        "name", "Washington, D.C.",
        "population", "672228",
        "wikidata", "Q61",
        "capital", "yes"
      ))));
  }

  @Test
  public void testStateCapital() {
    process(SimpleFeature.create(
      newPoint(0, 0),
      Map.of(
        "name", "Boston",
        "scalerank", 2,
        "wikidataid", "Q100"
      ),
      NATURAL_EARTH_SOURCE,
      "ne_10m_populated_places",
      0
    ));
    assertFeatures(0, List.of(Map.of(
      "_layer", "place",
      "class", "city",
      "name", "Boston",
      "rank", 3,
      "capital", 4,

      "_type", "point",
      "_minzoom", 3
    )), process(pointFeature(
      Map.of(
        "place", "city",
        "name", "Boston",
        "population", "667137",
        "capital", "4"
      ))));
    // no match when far away
    assertFeatures(0, List.of(Map.of(
      "_layer", "place",
      "class", "city",
      "name", "Boston",
      "rank", "<null>"
    )), process(SimpleFeature.create(
      newPoint(1, 1),
      Map.of(
        "place", "city",
        "name", "Boston",
        "wikidata", "Q100",
        "population", "667137",
        "capital", "4"
      ),
      OSM_SOURCE,
      null,
      0
    )));
    // unaccented name match
    assertFeatures(0, List.of(Map.of(
      "_layer", "place",
      "class", "city",
      "rank", 3
    )), process(pointFeature(
      Map.of(
        "place", "city",
        "name", "Böston",
        "population", "667137",
        "capital", "4"
      ))));
    // wikidata only match
    assertFeatures(0, List.of(Map.of(
      "_layer", "place",
      "class", "city",
      "rank", 3
    )), process(pointFeature(
      Map.of(
        "place", "city",
        "name", "Other name",
        "population", "667137",
        "wikidata", "Q100",
        "capital", "4"
      ))));
  }


  @Test
  public void testCityWithoutNaturalEarthMatch() {
    assertFeatures(7, List.of(Map.of(
      "_layer", "place",
      "class", "city",
      "rank", "<null>",
      "_minzoom", 7,
      "_labelgrid_limit", 4,
      "_labelgrid_size", 128d
    )), process(pointFeature(
      Map.of(
        "place", "city",
        "name", "City name"
      ))));
    assertFeatures(13, List.of(Map.of(
      "_layer", "place",
      "class", "isolated_dwelling",
      "rank", "<null>",
      "_labelgrid_limit", 0,
      "_labelgrid_size", 0d,
      "_minzoom", 14
    )), process(pointFeature(
      Map.of(
        "place", "isolated_dwelling",
        "name", "City name"
      ))));
    assertFeatures(12, List.of(Map.of(
      "_layer", "place",
      "class", "isolated_dwelling",
      "rank", "<null>",
      "_labelgrid_limit", 14,
      "_labelgrid_size", 128d,
      "_minzoom", 14
    )), process(pointFeature(
      Map.of(
        "place", "isolated_dwelling",
        "name", "City name"
      ))));
  }

  @Test
  public void testCitySetRankFromGridrank() throws GeometryException {
    var layerName = Place.LAYER_NAME;
    assertEquals(List.of(), profile.postProcessLayerFeatures(layerName, 13, List.of()));

    assertEquals(List.of(pointFeature(
      layerName,
      Map.of("rank", 11),
      1
    )), profile.postProcessLayerFeatures(layerName, 13, List.of(pointFeature(
      layerName,
      Map.of(),
      1
    ))));

    assertEquals(List.of(
      pointFeature(
        layerName,
        Map.of("rank", 11, "name", "a"),
        1
      ), pointFeature(
        layerName,
        Map.of("rank", 12, "name", "b"),
        1
      ), pointFeature(
        layerName,
        Map.of("rank", 11, "name", "c"),
        2
      )
    ), profile.postProcessLayerFeatures(layerName, 13, List.of(
      pointFeature(
        layerName,
        Map.of("name", "a"),
        1
      ),
      pointFeature(
        layerName,
        Map.of("name", "b"),
        1
      ),
      pointFeature(
        layerName,
        Map.of("name", "c"),
        2
      )
    )));
  }
}
