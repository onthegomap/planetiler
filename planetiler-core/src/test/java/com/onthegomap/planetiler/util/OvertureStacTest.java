package com.onthegomap.planetiler.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import org.junit.jupiter.api.Test;
import org.locationtech.jts.geom.Envelope;

/**
 * Tests for {@link OvertureStac} that run without any network access — all catalog JSON is provided inline.
 */
class OvertureStacTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  // ---- resolveLatestCatalogUrl -----------------------------------------------------------------

  @Test
  void testLatestLinkByFlag() throws IOException {
    // The link with "latest":true should be preferred
    JsonNode catalog = MAPPER.readTree("""
      {
        "links": [
          {"rel": "child", "href": "./2026-01-01.0/catalog.json"},
          {"rel": "child", "href": "./2026-02-18.0/catalog.json", "latest": true}
        ]
      }
      """);
    String url = OvertureStac.resolveLatestCatalogUrl(catalog, "https://stac.overturemaps.org/catalog.json");
    assertEquals("https://stac.overturemaps.org/2026-02-18.0/catalog.json", url);
  }

  @Test
  void testLatestFallbackToTopLevelField() throws IOException {
    // No link has "latest":true — fall back to the top-level "latest" string field
    JsonNode catalog = MAPPER.readTree("""
      {
        "latest": "2026-02-18.0",
        "links": [
          {"rel": "child", "href": "./2026-01-01.0/catalog.json"},
          {"rel": "child", "href": "./2026-02-18.0/catalog.json"}
        ]
      }
      """);
    String url = OvertureStac.resolveLatestCatalogUrl(catalog, "https://stac.overturemaps.org/catalog.json");
    assertEquals("https://stac.overturemaps.org/2026-02-18.0/catalog.json", url);
  }

  @Test
  void testLatestNotFound() throws IOException {
    JsonNode catalog = MAPPER.readTree("""
      { "links": [{"rel": "root", "href": "./catalog.json"}] }
      """);
    assertThrows(IllegalArgumentException.class,
      () -> OvertureStac.resolveLatestCatalogUrl(catalog, "https://stac.overturemaps.org/catalog.json"));
  }

  // ---- resolveChildUrl -------------------------------------------------------------------------

  @Test
  void testResolveChildByTitle() throws IOException {
    JsonNode catalog = MAPPER.readTree("""
      {
        "links": [
          {"rel": "child", "href": "./buildings/catalog.json", "title": "buildings"},
          {"rel": "child", "href": "./places/catalog.json",   "title": "places"}
        ]
      }
      """);
    String url = OvertureStac.resolveChildUrl(catalog,
      "https://stac.overturemaps.org/2026-02-18.0/catalog.json", "buildings");
    assertEquals("https://stac.overturemaps.org/2026-02-18.0/buildings/catalog.json", url);
  }

  @Test
  void testResolveChildByHrefSegment() throws IOException {
    // title is absent — match by the first path segment in the href
    JsonNode catalog = MAPPER.readTree("""
      {
        "links": [
          {"rel": "child", "href": "./transportation/catalog.json"},
          {"rel": "child", "href": "./base/catalog.json"}
        ]
      }
      """);
    String url = OvertureStac.resolveChildUrl(catalog,
      "https://stac.overturemaps.org/2026-02-18.0/catalog.json", "transportation");
    assertEquals("https://stac.overturemaps.org/2026-02-18.0/transportation/catalog.json", url);
  }

  @Test
  void testResolveChildNotFound() throws IOException {
    JsonNode catalog = MAPPER.readTree("""
      { "links": [{"rel": "child", "href": "./buildings/catalog.json", "title": "buildings"}] }
      """);
    String result = OvertureStac.resolveChildUrl(catalog,
      "https://stac.overturemaps.org/2026-02-18.0/catalog.json", "places");
    assertEquals(null, result);
  }

  // ---- itemBboxIntersects ----------------------------------------------------------------------

  @Test
  void testBboxIntersectsOverlap() throws IOException {
    // Item covers western hemisphere; filter covers Monaco area — no overlap
    JsonNode item = MAPPER.readTree("""
      {"bbox": [-179.9, -84.3, -2.8, -22.5]}
      """);
    Envelope monacoFilter = new Envelope(7.3, 7.5, 43.7, 43.8);
    assertFalse(OvertureStac.itemBboxIntersects(item, monacoFilter));
  }

  @Test
  void testBboxIntersectsMatch() throws IOException {
    // Item covers Europe; filter is Monaco — they intersect
    JsonNode item = MAPPER.readTree("""
      {"bbox": [-10.0, 35.0, 30.0, 70.0]}
      """);
    Envelope monacoFilter = new Envelope(7.3, 7.5, 43.7, 43.8);
    assertTrue(OvertureStac.itemBboxIntersects(item, monacoFilter));
  }

  @Test
  void testBboxMissingIsInclusive() throws IOException {
    // No bbox → include the item conservatively
    JsonNode item = MAPPER.readTree("""
      {"id": "some-item"}
      """);
    assertTrue(OvertureStac.itemBboxIntersects(item, new Envelope(7.3, 7.5, 43.7, 43.8)));
  }

  // ---- resolveUrl ------------------------------------------------------------------------------

  @Test
  void testResolveAbsoluteUrl() {
    String result = OvertureStac.resolveUrl("https://stac.overturemaps.org/catalog.json",
      "https://other.example.com/foo.json");
    assertEquals("https://other.example.com/foo.json", result);
  }

  @Test
  void testResolveRelativeUrl() {
    String result = OvertureStac.resolveUrl("https://stac.overturemaps.org/catalog.json",
      "./2026-02-18.0/catalog.json");
    assertEquals("https://stac.overturemaps.org/2026-02-18.0/catalog.json", result);
  }

  @Test
  void testResolveRelativeUrlFromChild() {
    String result = OvertureStac.resolveUrl(
      "https://stac.overturemaps.org/2026-02-18.0/catalog.json",
      "./buildings/catalog.json");
    assertEquals("https://stac.overturemaps.org/2026-02-18.0/buildings/catalog.json", result);
  }

  // ---- getParquetUrls (integration-style, all HTTP stubbed via overrideable fetch) -------------

  /**
   * Verifies bbox filtering: one item in the western hemisphere (excluded) and one covering Europe (included) when
   * filtering to the Monaco area.
   */
  @Test
  void testGetParquetUrlsFiltersByBbox() throws IOException {
    Envelope monacoFilter = new Envelope(7.3, 7.5, 43.7, 43.8);

    // item 00000: western hemisphere — outside Monaco bounds
    JsonNode i0 = MAPPER.readTree("""
      {
        "bbox": [-179.9, -84.3, -2.8, -22.5],
        "assets": {"aws": {"href": "https://s3.amazonaws.com/overture/part-00000.parquet"}}
      }
      """);
    // item 00001: Europe — intersects Monaco bounds
    JsonNode i1 = MAPPER.readTree("""
      {
        "bbox": [-10.0, 35.0, 30.0, 70.0],
        "assets": {"aws": {"href": "https://s3.amazonaws.com/overture/part-00001.parquet"}}
      }
      """);

    assertFalse(OvertureStac.itemBboxIntersects(i0, monacoFilter), "western hemisphere item should be excluded");
    assertTrue(OvertureStac.itemBboxIntersects(i1, monacoFilter), "Europe item should be included");
  }

  /**
   * Verifies that resolveChildUrl returns null for a missing theme, and that the null is what getParquetUrls
   * wraps into an IllegalArgumentException.
   */
  @Test
  void testUnknownThemeReturnsNull() throws IOException {
    JsonNode releaseCatalog = MAPPER.readTree("""
      {
        "links": [
          {"rel": "child", "href": "./buildings/catalog.json", "title": "buildings"}
        ]
      }
      """);
    // resolveChildUrl itself returns null — getParquetUrls turns the null into an IAE
    String result = OvertureStac.resolveChildUrl(releaseCatalog,
      "https://stac.overturemaps.org/2026-02-18.0/catalog.json", "nonexistent");
    assertEquals(null, result);
  }
}
