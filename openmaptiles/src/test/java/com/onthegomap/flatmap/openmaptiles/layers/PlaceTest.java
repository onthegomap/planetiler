package com.onthegomap.flatmap.openmaptiles.layers;

import static com.onthegomap.flatmap.TestUtils.newPoint;
import static com.onthegomap.flatmap.TestUtils.rectangle;
import static com.onthegomap.flatmap.openmaptiles.OpenMapTilesProfile.NATURAL_EARTH_SOURCE;
import static com.onthegomap.flatmap.openmaptiles.OpenMapTilesProfile.OSM_SOURCE;

import com.onthegomap.flatmap.geo.GeoUtils;
import com.onthegomap.flatmap.read.ReaderFeature;
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
    process(new ReaderFeature(
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
    )), process(new ReaderFeature(
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
    )), process(new ReaderFeature(
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
    process(new ReaderFeature(
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

    process(new ReaderFeature(
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
    assertFeatures(0, List.of(), process(new ReaderFeature(
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
    assertFeatures(0, List.of(), process(new ReaderFeature(
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
    )), process(new ReaderFeature(
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
    )), process(lineFeature(
      Map.of(
        "place", "island",
        "name", "Nantucket",
        "name:en", "Nantucket"
      ))));
  }

  @Test
  public void testIslandPolygon() {
    double rank3area = Math.pow(GeoUtils.metersToPixelAtEquator(0, Math.sqrt(40_000_000 + 1)) / 256d, 2);
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
}
