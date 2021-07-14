package com.onthegomap.flatmap.openmaptiles.layers;

import static com.onthegomap.flatmap.TestUtils.newPoint;
import static com.onthegomap.flatmap.TestUtils.rectangle;
import static com.onthegomap.flatmap.openmaptiles.OpenMapTilesProfile.NATURAL_EARTH_SOURCE;
import static com.onthegomap.flatmap.openmaptiles.OpenMapTilesProfile.OSM_SOURCE;

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
}
