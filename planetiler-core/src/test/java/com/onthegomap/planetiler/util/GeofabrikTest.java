package com.onthegomap.planetiler.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

public class GeofabrikTest {

  private static final byte[] response =
      """
    { "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "properties": {
                    "id" : "afghanistan",
                    "parent" : "asia",
                    "iso3166-1:alpha2" : [ "AF" ],
                    "name" : "Afghanistan",
                    "urls" : {
                        "pbf" : "https://download.geofabrik.de/asia/afghanistan-latest.osm.pbf",
                        "bz2" : "https://download.geofabrik.de/asia/afghanistan-latest.osm.bz2",
                        "shp" : "https://download.geofabrik.de/asia/afghanistan-latest-free.shp.zip",
                        "pbf-internal" : "https://osm-internal.download.geofabrik.de/asia/afghanistan-latest-internal.osm.pbf",
                        "history" : "https://osm-internal.download.geofabrik.de/asia/afghanistan-internal.osh.pbf",
                        "taginfo" : "https://taginfo.geofabrik.de/asia/afghanistan/",
                        "updates" : "https://download.geofabrik.de/asia/afghanistan-updates"
                    }
                }
            }
        ]
    }
    """
          .getBytes(StandardCharsets.UTF_8);

  @Test
  public void testFound() throws IOException {
    var index = Geofabrik.parseIndexJson(new ByteArrayInputStream(response));
    String url = Geofabrik.searchIndexForDownloadUrl("afghanistan", index);
    assertEquals("https://download.geofabrik.de/asia/afghanistan-latest.osm.pbf", url);
  }

  @Test
  public void testNotFound() throws IOException {
    var index = Geofabrik.parseIndexJson(new ByteArrayInputStream(response));
    assertThrows(
        IllegalArgumentException.class, () -> Geofabrik.searchIndexForDownloadUrl("monaco", index));
  }
}
