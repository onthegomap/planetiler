package com.onthegomap.planetiler.archive;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.onthegomap.planetiler.Profile;
import com.onthegomap.planetiler.config.Arguments;
import com.onthegomap.planetiler.config.PlanetilerConfig;
import com.onthegomap.planetiler.geo.GeoUtils;
import java.util.Map;
import java.util.TreeMap;
import org.junit.jupiter.api.Test;
import org.locationtech.jts.geom.CoordinateXY;
import org.locationtech.jts.geom.Envelope;

class TileArchiveMetadataTest {

  @Test
  void testAddMetadataWorldBounds() {
    var bounds = GeoUtils.WORLD_LAT_LON_BOUNDS;
    var metadata = new TileArchiveMetadata(new Profile.NullProfile(), PlanetilerConfig.from(Arguments.of(Map.of(
      "bounds", bounds.getMinX() + "," + bounds.getMinY() + "," + bounds.getMaxX() + "," + bounds.getMaxY()
    ))));
    assertEquals(bounds, metadata.bounds());
    assertEquals(new CoordinateXY(0, 0), metadata.center());
    assertEquals(0d, metadata.zoom().doubleValue());
  }

  @Test
  void testAddMetadataSmallBounds() {
    var bounds = new Envelope(-73.6632, -69.7598, 41.1274, 43.0185);
    var metadata = new TileArchiveMetadata(new Profile.NullProfile(), PlanetilerConfig.from(Arguments.of(Map.of(
      "bounds", "-73.6632,41.1274,-69.7598,43.0185"
    ))));
    assertEquals(bounds, metadata.bounds());
    assertEquals(-71.7115, metadata.center().x, 1e-5);
    assertEquals(42.07295, metadata.center().y, 1e-5);
    assertEquals(7, Math.ceil(metadata.zoom()));
  }

  @Test
  void testToMap() {
    var bounds = "-73.6632,41.1274,-69.7598,43.0185";
    var metadata = new TileArchiveMetadata(
      new Profile.NullProfile(),
      PlanetilerConfig.from(Arguments.of(Map.of(
        "bounds", bounds
      ))));
    var map = new TreeMap<>(metadata.toMap());
    assertNotNull(map.remove("planetiler:version"));
    map.remove("planetiler:githash");
    map.remove("planetiler:buildtime");
    assertEquals(
      new TreeMap<>(Map.of(
        "name", "Null",
        "type", "baselayer",
        "format", "pbf",
        "zoom", "6.5271217861412305",
        "minzoom", "0",
        "maxzoom", "14",
        "bounds", "-73.6632,41.1274,-69.7598,43.0185",
        "center", "-71.7115,42.07295",
        "compression", "gzip"
      )),
      map
    );
  }
}
