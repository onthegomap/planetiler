package com.onthegomap.planetiler.examples;

import static com.onthegomap.planetiler.TestUtils.assertContains;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.onthegomap.planetiler.TestUtils;
import com.onthegomap.planetiler.config.Arguments;
import com.onthegomap.planetiler.geo.GeoUtils;
import com.onthegomap.planetiler.mbtiles.Mbtiles;
import com.onthegomap.planetiler.reader.SourceFeature;
import com.onthegomap.planetiler.reader.osm.OsmElement;
import com.onthegomap.planetiler.reader.osm.OsmSourceFeature;
import java.nio.file.Path;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;

class OsmQaTilesTest {

  private final OsmQaTiles profile = new OsmQaTiles();

  @Test
  void testNode() {
    Map<String, Object> tags = Map.of("key", "value");
    class TestNode extends SourceFeature implements OsmSourceFeature {
      TestNode() {
        super(tags, "source", "layer", null, 0);
      }

      @Override
      public Geometry latLonGeometry() {
        return null;
      }

      @Override
      public Geometry worldGeometry() {
        return TestUtils.newPoint(
          0.5, 0.5
        );
      }

      @Override
      public boolean isPoint() {
        return true;
      }

      @Override
      public boolean canBePolygon() {
        return false;
      }

      @Override
      public boolean canBeLine() {
        return false;
      }

      @Override
      public OsmElement originalElement() {
        return new OsmElement.Node(123, tags, 1, 1, new OsmElement.Info(1, 2, 3, 4, "user"));
      }
    }

    var node = new TestNode();
    var mapFeatures = TestUtils.processSourceFeature(node, profile);

    // verify output attributes
    assertEquals(1, mapFeatures.size());
    var feature = mapFeatures.get(0);
    assertEquals("osm", feature.getLayer());
    assertEquals(Map.of(
      "key", "value",
      "@changeset", 1L,
      "@timestamp", 2L,
      "@id", 0L,
      "@type", "node",
      "@uid", 3,
      "@user", "user",
      "@version", 4
    ), feature.getAttrsAtZoom(12));
    assertEquals(0, feature.getBufferPixelsAtZoom(12), 1e-2);
  }

  @Test
  void integrationTest(@TempDir Path tmpDir) throws Exception {
    Path dbPath = tmpDir.resolve("output.mbtiles");
    OsmQaTiles.run(Arguments.of(
      // Override input source locations
      "osm_path", TestUtils.pathToResource("monaco-latest.osm.pbf"),
      // Override temp dir location
      "tmp", tmpDir.toString(),
      // Override output location
      "output", dbPath.toString()
    ));
    try (Mbtiles mbtiles = Mbtiles.newReadOnlyDatabase(dbPath)) {
      Map<String, String> metadata = mbtiles.metadataTable().getAll();
      assertEquals("osm qa", metadata.get("name"));
      assertContains("openstreetmap.org/copyright", metadata.get("attribution"));

      TestUtils
        .assertNumFeatures(mbtiles, "osm", 12, Map.of(
          "bus", "yes",
          "name", "Crémaillère",
          "public_transport", "stop_position",
          "@type", "node",
          "@version", 4L,
          "@id", 16997778331L
        ), GeoUtils.WORLD_LAT_LON_BOUNDS, 1, Point.class);
      TestUtils
        .assertNumFeatures(mbtiles, "osm", 12, Map.of(
          "boundary", "administrative",
          "admin_level", "10",
          "name", "Monte-Carlo",
          "wikipedia", "fr:Monte-Carlo",
          "ISO3166-2", "MC-MC",
          "type", "boundary",
          "wikidata", "Q45240",
          "@type", "relation",
          "@version", 9L,
          "@id", 59864383L
        ), GeoUtils.WORLD_LAT_LON_BOUNDS, 1, Polygon.class);
      TestUtils
        .assertNumFeatures(mbtiles, "osm", 12, Map.of(
          "name", "Avenue de la Costa",
          "maxspeed", "50",
          "lit", "yes",
          "surface", "asphalt",
          "lanes", "2",
          "@type", "way",
          "@version", 5L,
          "@id", 1660097912L
        ), GeoUtils.WORLD_LAT_LON_BOUNDS, 1, LineString.class);
    }
  }
}
