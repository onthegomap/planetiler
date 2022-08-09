package com.onthegomap.planetiler.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class GeofabrikTest {

  private static final byte[] response =
    """
      {
        "type": "FeatureCollection",
        "features": [
          {
            "type": "Feature",
            "properties": {
              "id": "afghanistan",
              "parent": "asia",
              "iso3166-1:alpha2": [
                "AF"
              ],
              "name": "Afghanistan",
              "urls": {
                "pbf": "https://download.geofabrik.de/asia/afghanistan-latest.osm.pbf",
                "bz2": "https://download.geofabrik.de/asia/afghanistan-latest.osm.bz2",
                "shp": "https://download.geofabrik.de/asia/afghanistan-latest-free.shp.zip",
                "pbf-internal": "https://osm-internal.download.geofabrik.de/asia/afghanistan-latest-internal.osm.pbf",
                "history": "https://osm-internal.download.geofabrik.de/asia/afghanistan-internal.osh.pbf",
                "taginfo": "https://taginfo.geofabrik.de/asia/afghanistan/",
                "updates": "https://download.geofabrik.de/asia/afghanistan-updates"
              }
            }
          },
          {
            "type": "Feature",
            "properties": {
              "id": "georgia",
              "parent": "europe",
              "iso3166-1:alpha2": [
                "GE"
              ],
              "name": "Georgia",
              "urls": {
                "pbf": "https://download.geofabrik.de/europe/georgia-latest.osm.pbf",
                "bz2": "https://download.geofabrik.de/europe/georgia-latest.osm.bz2",
                "shp": "https://download.geofabrik.de/europe/georgia-latest-free.shp.zip",
                "pbf-internal": "https://osm-internal.download.geofabrik.de/europe/georgia-latest-internal.osm.pbf",
                "history": "https://osm-internal.download.geofabrik.de/europe/georgia-internal.osh.pbf",
                "taginfo": "https://taginfo.geofabrik.de/europe/georgia/",
                "updates": "https://download.geofabrik.de/europe/georgia-updates"
              }
            }
          },
          {
            "type": "Feature",
            "properties": {
              "id": "us/georgia",
              "parent": "north-america",
              "iso3166-2": [
                "US-GA"
              ],
              "name": "Georgia",
              "urls": {
                "pbf": "https://download.geofabrik.de/north-america/us/georgia-latest.osm.pbf",
                "bz2": "https://download.geofabrik.de/north-america/us/georgia-latest.osm.bz2",
                "shp": "https://download.geofabrik.de/north-america/us/georgia-latest-free.shp.zip",
                "pbf-internal": "https://osm-internal.download.geofabrik.de/north-america/us/georgia-latest-internal.osm.pbf",
                "history": "https://osm-internal.download.geofabrik.de/north-america/us/georgia-internal.osh.pbf",
                "taginfo": "https://taginfo.geofabrik.de/north-america/us/georgia/",
                "updates": "https://download.geofabrik.de/north-america/us/georgia-updates"
              }
            }
          },
          {
            "type": "Feature",
            "properties": {
              "id": "us/massachusetts",
              "parent": "north-america",
              "iso3166-2": [
                "US-MA"
              ],
              "name": "us/massachusetts",
              "urls": {
                "pbf": "https://download.geofabrik.de/north-america/us/massachusetts-latest.osm.pbf",
                "bz2": "https://download.geofabrik.de/north-america/us/massachusetts-latest.osm.bz2",
                "shp": "https://download.geofabrik.de/north-america/us/massachusetts-latest-free.shp.zip",
                "pbf-internal": "https://osm-internal.download.geofabrik.de/north-america/us/massachusetts-latest-internal.osm.pbf",
                "history": "https://osm-internal.download.geofabrik.de/north-america/us/massachusetts-internal.osh.pbf",
                "taginfo": "https://taginfo.geofabrik.de/north-america/us/massachusetts/",
                "updates": "https://download.geofabrik.de/north-america/us/massachusetts-updates"
              }
            }
          },
          {
            "type": "Feature",
            "properties": {
              "id": "us/west-virginia",
              "parent": "north-america",
              "iso3166-2": [
                "US-WV"
              ],
              "name": "us/west-virginia",
              "urls": {
                "pbf": "https://download.geofabrik.de/north-america/us/west-virginia-latest.osm.pbf",
                "bz2": "https://download.geofabrik.de/north-america/us/west-virginia-latest.osm.bz2",
                "shp": "https://download.geofabrik.de/north-america/us/west-virginia-latest-free.shp.zip",
                "pbf-internal": "https://osm-internal.download.geofabrik.de/north-america/us/west-virginia-latest-internal.osm.pbf",
                "history": "https://osm-internal.download.geofabrik.de/north-america/us/west-virginia-internal.osh.pbf",
                "taginfo": "https://taginfo.geofabrik.de/north-america/us/west-virginia/",
                "updates": "https://download.geofabrik.de/north-america/us/west-virginia-updates"
              }
            }
          },
          {
            "type": "Feature",
            "properties": {
              "id": "west-sussex",
              "parent": "england",
              "name": "West Sussex",
              "urls": {
                "pbf": "https://download.geofabrik.de/europe/great-britain/england/west-sussex-latest.osm.pbf",
                "bz2": "https://download.geofabrik.de/europe/great-britain/england/west-sussex-latest.osm.bz2",
                "shp": "https://download.geofabrik.de/europe/great-britain/england/west-sussex-latest-free.shp.zip",
                "pbf-internal": "https://osm-internal.download.geofabrik.de/europe/great-britain/england/west-sussex-latest-internal.osm.pbf",
                "history": "https://osm-internal.download.geofabrik.de/europe/great-britain/england/west-sussex-internal.osh.pbf",
                "taginfo": "https://taginfo.geofabrik.de/europe/great-britain/england/west-sussex/",
                "updates": "https://download.geofabrik.de/europe/great-britain/england/west-sussex-updates"
              }
            }
          }
        ]
      }
      """
      .getBytes(StandardCharsets.UTF_8);

  @ParameterizedTest
  @CsvSource({
    "afghanistan,https://download.geofabrik.de/asia/afghanistan-latest.osm.pbf",
    "af,https://download.geofabrik.de/asia/afghanistan-latest.osm.pbf",
    "ge,https://download.geofabrik.de/europe/georgia-latest.osm.pbf",
    "us-ga,https://download.geofabrik.de/north-america/us/georgia-latest.osm.pbf",
    "us ga,https://download.geofabrik.de/north-america/us/georgia-latest.osm.pbf",
    "us/ga,https://download.geofabrik.de/north-america/us/georgia-latest.osm.pbf",
    "us_ga,https://download.geofabrik.de/north-america/us/georgia-latest.osm.pbf",
    "us/georgia,https://download.geofabrik.de/north-america/us/georgia-latest.osm.pbf",
    "west virginia,https://download.geofabrik.de/north-america/us/west-virginia-latest.osm.pbf",
    "us/west-virginia,https://download.geofabrik.de/north-america/us/west-virginia-latest.osm.pbf",
    "west sussex,https://download.geofabrik.de/europe/great-britain/england/west-sussex-latest.osm.pbf"
  })
  void testFound(String search, String expectedUrl) throws IOException {
    var index = Geofabrik.parseIndexJson(new ByteArrayInputStream(response));
    String url = Geofabrik.searchIndexForDownloadUrl(search, index);
    assertEquals(expectedUrl, url);
  }

  @Test
  void testNotFound() throws IOException {
    var index = Geofabrik.parseIndexJson(new ByteArrayInputStream(response));
    assertThrows(IllegalArgumentException.class,
      () -> Geofabrik.searchIndexForDownloadUrl("monaco", index));
  }

  @Test
  void testAmbiguous() throws IOException {
    var index = Geofabrik.parseIndexJson(new ByteArrayInputStream(response));
    assertThrows(IllegalArgumentException.class,
      () -> Geofabrik.searchIndexForDownloadUrl("georgia", index));
  }
}
