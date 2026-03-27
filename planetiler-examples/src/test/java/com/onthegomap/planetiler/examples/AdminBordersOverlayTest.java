package com.onthegomap.planetiler.examples;

import static com.onthegomap.planetiler.TestUtils.assertContains;
import static com.onthegomap.planetiler.TestUtils.newLineString;
import static com.onthegomap.planetiler.TestUtils.newPolygon;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.onthegomap.planetiler.FeatureCollector;
import com.onthegomap.planetiler.TestUtils;
import com.onthegomap.planetiler.VectorTile;
import com.onthegomap.planetiler.config.Arguments;
import com.onthegomap.planetiler.geo.GeoUtils;
import com.onthegomap.planetiler.mbtiles.Mbtiles;
import com.onthegomap.planetiler.mbtiles.Verify;
import com.onthegomap.planetiler.reader.SimpleFeature;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.Point;

class AdminBordersOverlayTest {

  private record ZoomCase(int adminLevel, String kind, int borderMinZoom, int labelMinZoom, int maxLabelZoom) {}

  private static final List<ZoomCase> ZOOM_CASES = List.of(
    new ZoomCase(2, "country", 0, 2, 5),
    new ZoomCase(3, "region", 4, 6, 8),
    new ZoomCase(4, "region", 4, 6, 8),
    new ZoomCase(5, "county", 7, 9, 10),
    new ZoomCase(6, "county", 7, 9, 10),
    new ZoomCase(7, "city", 9, 11, 12),
    new ZoomCase(8, "city", 11, 12, 13),
    new ZoomCase(9, "local", 12, 12, 14),
    new ZoomCase(10, "local", 12, 12, 14)
  );

  private final AdminBordersOverlay profile = new AdminBordersOverlay();

  @Test
  void testCountryBoundaryFeature() {
    var countryBoundary = SimpleFeature.create(
      TestUtils.newLineString(10, 20, 30, 40),
      Map.of("boundary", "administrative", "admin_level", "2", "name", "Sample Border")
    );

    List<FeatureCollector.Feature> mapFeatures = TestUtils.processSourceFeature(countryBoundary, profile);

    assertEquals(1, mapFeatures.size(), "country boundary should produce exactly 1 feature");
    var feature = mapFeatures.getFirst();
    assertEquals("admin_borders-country", feature.getLayer(),
      "country boundary should be emitted as 'admin_borders-country' layer");
    assertEquals(0, feature.getMinZoom(), "country boundaries should be visible from zoom 0");
    assertEquals(14, feature.getMaxZoom(), "country boundaries should be visible up to zoom 14");
    assertEquals(Map.of(
      "admin_level", 2,
      "kind", "country",
      "name", "Sample Border",
      "maritime", false
    ), feature.getAttrsAtZoom(8));
  }

  @Test
  void testCountryLabelComesFromPolygonCenter() {
    var countryPolygon = SimpleFeature.create(
      newPolygon(0, 0, 3, 0, 3, 3, 0, 3, 0, 0),
      Map.of("boundary", "administrative", "admin_level", "2", "name", "Monaco")
    );

    List<FeatureCollector.Feature> mapFeatures = TestUtils.processSourceFeature(countryPolygon, profile);

    assertEquals(1, mapFeatures.size(), "country polygon should produce exactly 1 label feature");
    var label = mapFeatures.getFirst();
    assertEquals("polygon_area_labels-country", label.getLayer(),
      "country polygon label should be emitted as 'polygon_area_labels-country' layer");
    assertEquals(2, label.getMinZoom(), "country labels should start appearing at zoom 2");
    assertEquals(5, label.getMaxZoom(), "country labels should not appear after zoom 5");
    assertEquals(Map.of(
      "admin_level", 2,
      "kind", "country",
      "name", "Monaco",
      "label_level", "Country",
      "label_text", "Country: Monaco"
    ), label.getAttrsAtZoom(8), "country label attributes at zoom 8 should contain expected values");
  }

  @Test
  void testBorderZoomRangesForAllAdminLevels() {
    for (ZoomCase zoomCase : ZOOM_CASES) {
      var boundary = SimpleFeature.create(
        newLineString(10, 20, 30, 40),
        Map.of(
          "boundary", "administrative",
          "admin_level", Integer.toString(zoomCase.adminLevel),
          "name", "L" + zoomCase.adminLevel
        )
      );
      List<FeatureCollector.Feature> features = TestUtils.processSourceFeature(boundary, profile);

      assertEquals(1, features.size(), "expected 1 border feature for admin_level=" + zoomCase.adminLevel);
      FeatureCollector.Feature border = features.getFirst();
      assertEquals("admin_borders-" + zoomCase.kind, border.getLayer(),
        "border layer name mismatch for admin_level=" + zoomCase.adminLevel + " (expected kind=" + zoomCase.kind + ")");
      assertEquals(zoomCase.borderMinZoom, border.getMinZoom(),
        "min zoom mismatch for admin_level=" + zoomCase.adminLevel + " (expected=" + zoomCase.borderMinZoom + ", got=" +
          border.getMinZoom() + ")");
      assertEquals(14, border.getMaxZoom(),
        "max zoom mismatch for admin_level=" + zoomCase.adminLevel + " (expected=14, got=" + border.getMaxZoom() + ")");
    }
  }

  @Test
  void testLabelZoomRangesForAllAdminLevels() {
    for (ZoomCase zoomCase : ZOOM_CASES) {
      var polygon = SimpleFeature.create(
        newPolygon(0, 0, 3, 0, 3, 3, 0, 3, 0, 0),
        Map.of(
          "boundary", "administrative",
          "admin_level", Integer.toString(zoomCase.adminLevel),
          "name", "A" + zoomCase.adminLevel
        )
      );
      List<FeatureCollector.Feature> features = TestUtils.processSourceFeature(polygon, profile);

      assertEquals(1, features.size(), "expected 1 label feature for admin_level=" + zoomCase.adminLevel);
      FeatureCollector.Feature label = features.getFirst();
      assertEquals("polygon_area_labels-" + zoomCase.kind, label.getLayer(),
        "label layer name mismatch for admin_level=" + zoomCase.adminLevel + " (expected kind=" + zoomCase.kind + ")");
      assertEquals(zoomCase.labelMinZoom, label.getMinZoom(),
        "label min zoom mismatch for admin_level=" + zoomCase.adminLevel + " (expected=" + zoomCase.labelMinZoom +
          ", got=" + label.getMinZoom() + ")");
      assertEquals(zoomCase.maxLabelZoom, label.getMaxZoom(),
        "label max zoom mismatch for admin_level=" + zoomCase.adminLevel + " (expected=" + zoomCase.maxLabelZoom +
          ", got=" + label.getMaxZoom() + ")");
      assertEquals(zoomCase.kind, label.getAttrsAtZoom(zoomCase.labelMinZoom).get("kind"),
        "kind attribute mismatch for admin_level=" + zoomCase.adminLevel + " at min zoom");
    }
  }

  @Test
  void testLabelForZoom11OnlyIncludesCityAndLocalKinds() {
    for (ZoomCase zoomCase : ZOOM_CASES) {
      var polygon = SimpleFeature.create(
        newPolygon(0, 0, 3, 0, 3, 3, 0, 3, 0, 0),
        Map.of(
          "boundary", "administrative",
          "admin_level", Integer.toString(zoomCase.adminLevel),
          "name", "Z" + zoomCase.adminLevel
        )
      );
      List<FeatureCollector.Feature> features = TestUtils.processSourceFeature(polygon, profile);
      FeatureCollector.Feature label = features.getFirst();
      boolean visibleAtZ11 = 11 >= label.getMinZoom() && 11 <= label.getMaxZoom();
      if (zoomCase.adminLevel == 7) {
        assertTrue(visibleAtZ11, "admin_level=" + zoomCase.adminLevel + " should be visible at z11");
      } else {
        assertFalse(visibleAtZ11, "admin_level=" + zoomCase.adminLevel + " should be hidden at z11");
      }
    }
  }

  @Test
  void testIgnoresNonAdministrativeBoundaries() {
    var nonAdminBoundary = SimpleFeature.create(
      TestUtils.newLineString(10, 20, 30, 40),
      Map.of("boundary", "national_park", "admin_level", "2")
    );

    List<FeatureCollector.Feature> mapFeatures = TestUtils.processSourceFeature(nonAdminBoundary, profile);

    assertTrue(mapFeatures.isEmpty());
  }

  @Test
  void testTilePostProcessingMergesConnectedLines() {
    String layer = "admin_borders-country";
    Map<String, Object> attrs = Map.of("admin_level", 2, "kind", "country");
    var line1 = new VectorTile.Feature(layer, 1, VectorTile.encodeGeometry(newLineString(0, 0, 10, 0)), attrs);
    var line2 = new VectorTile.Feature(layer, 2, VectorTile.encodeGeometry(newLineString(10, 0, 20, 0)), attrs);
    var connected = new VectorTile.Feature(layer, 0, VectorTile.encodeGeometry(newLineString(0, 0, 20, 0)), attrs);

    List<VectorTile.Feature> result = profile.postProcessLayerFeatures(layer, 10, List.of(line1, line2));
    assertEquals(1, result.size(), "two connected lines should be merged into 1 line");
    assertEquals(
      List.of(connected),
      result,
      "connected line segments should be merged into a single continuous line"
    );
  }

  @Test
  void integrationTest(@TempDir Path tmpDir) throws Exception {
    Path dbPath = tmpDir.resolve("output.mbtiles");
    AdminBordersOverlay.run(Arguments.of(
      "osm_path", TestUtils.pathToResource("monaco-latest.osm.pbf"),
      "tmpdir", tmpDir.toString(),
      "output", dbPath.toString()
    ));

    try (Mbtiles mbtiles = Mbtiles.newReadOnlyDatabase(dbPath)) {
      Map<String, String> metadata = mbtiles.metadata().toMap();
      assertEquals("Administrative Borders Overlay", metadata.get("name"));
      assertContains("openstreetmap.org/copyright", metadata.get("attribution"));

      int z5Count = Verify.getNumFeatures(
        mbtiles,
        "admin_borders-country",
        5,
        Map.of(),
        GeoUtils.WORLD_LAT_LON_BOUNDS,
        LineString.class
      );
      int z11Borders = Verify.getNumFeatures(
        mbtiles,
        "admin_borders-city",
        11,
        Map.of(),
        GeoUtils.WORLD_LAT_LON_BOUNDS,
        LineString.class
      );
      int z13CityPolygonLabels = Verify.getNumFeatures(
        mbtiles,
        "polygon_area_labels-city",
        13,
        Map.of(),
        GeoUtils.WORLD_LAT_LON_BOUNDS,
        Point.class
      );

      assertTrue(z5Count > 0);
      assertTrue(z11Borders > 0, "expected city-level border lines at higher zoom");
      assertTrue(z13CityPolygonLabels > 0, "expected named city-level labels at zoom 13+");
      TestUtils.assertTileDuplicates(mbtiles, 0);
    }
  }
}
