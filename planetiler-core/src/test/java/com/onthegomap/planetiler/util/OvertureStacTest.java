package com.onthegomap.planetiler.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.onthegomap.planetiler.config.Bounds;
import com.onthegomap.planetiler.config.PlanetilerConfig;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.locationtech.jts.geom.Envelope;

/**
 * Tests for {@link OvertureStac} — all catalog JSON is provided inline; no real HTTP calls are made.
 */
class OvertureStacTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final PlanetilerConfig CONFIG = PlanetilerConfig.defaults();

  /** Reset the stub fetcher after every test so it doesn't leak between tests. */
  @AfterEach
  void resetFetcher() {
    OvertureStac.fetcher = null;
  }

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
    assertEquals(null, OvertureStac.resolveChildUrl(catalog,
      "https://stac.overturemaps.org/2026-02-18.0/catalog.json", "places"));
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

  // ---- getParquetUrls (stubbed HTTP) -----------------------------------------------------------

  /**
   * Happy path: 2 items, one outside Monaco bbox (excluded), one inside (included). Uses aws asset URL.
   */
  @Test
  void testGetParquetUrlsFiltersByBbox() {
    var stubs = Map.of(
      "https://stac.example.com/catalog.json", """
        {"latest": "2026-02-18.0", "links": [
          {"rel": "child", "href": "./2026-02-18.0/catalog.json", "latest": true}
        ]}""",
      "https://stac.example.com/2026-02-18.0/catalog.json", """
        {"links": [
          {"rel": "child", "href": "./buildings/catalog.json", "title": "buildings"}
        ]}""",
      "https://stac.example.com/2026-02-18.0/buildings/catalog.json", """
        {"links": [
          {"rel": "child", "href": "./building/collection.json", "title": "building"}
        ]}""",
      "https://stac.example.com/2026-02-18.0/buildings/building/collection.json", """
        {"links": [
          {"rel": "item", "href": "./item-west.json"},
          {"rel": "item", "href": "./item-europe.json"}
        ]}""",
      "https://stac.example.com/2026-02-18.0/buildings/building/item-west.json", """
        {"bbox": [-179.9, -84.3, -2.8, -22.5],
         "assets": {"aws": {"href": "https://s3.example.com/part-west.parquet"}}}""",
      "https://stac.example.com/2026-02-18.0/buildings/building/item-europe.json", """
        {"bbox": [-10.0, 35.0, 30.0, 70.0],
         "assets": {"aws": {"href": "https://s3.example.com/part-europe.parquet"}}}"""
    );
    OvertureStac.fetcher = stubs::get;

    Bounds monaco = new Bounds(new Envelope(7.3, 7.5, 43.7, 43.8));
    List<String> result = OvertureStac.getParquetUrls(
      "https://stac.example.com/catalog.json", "buildings", "building", monaco, CONFIG);

    assertEquals(List.of("https://s3.example.com/part-europe.parquet"), result);
  }

  /**
   * World bounds (no bbox filter) — both items should be returned.
   */
  @Test
  void testGetParquetUrlsWorldBoundsIncludesAll() {
    var stubs = Map.of(
      "https://stac.example.com/catalog.json", """
        {"latest": "2026-02-18.0", "links": [
          {"rel": "child", "href": "./2026-02-18.0/catalog.json", "latest": true}
        ]}""",
      "https://stac.example.com/2026-02-18.0/catalog.json", """
        {"links": [{"rel": "child", "href": "./buildings/catalog.json", "title": "buildings"}]}""",
      "https://stac.example.com/2026-02-18.0/buildings/catalog.json", """
        {"links": [{"rel": "child", "href": "./building/collection.json", "title": "building"}]}""",
      "https://stac.example.com/2026-02-18.0/buildings/building/collection.json", """
        {"links": [
          {"rel": "item", "href": "./item-a.json"},
          {"rel": "item", "href": "./item-b.json"}
        ]}""",
      "https://stac.example.com/2026-02-18.0/buildings/building/item-a.json", """
        {"bbox": [-179.9, -84.3, -2.8, -22.5],
         "assets": {"aws": {"href": "https://s3.example.com/part-a.parquet"}}}""",
      "https://stac.example.com/2026-02-18.0/buildings/building/item-b.json", """
        {"bbox": [-10.0, 35.0, 30.0, 70.0],
         "assets": {"aws": {"href": "https://s3.example.com/part-b.parquet"}}}"""
    );
    OvertureStac.fetcher = stubs::get;

    List<String> result = OvertureStac.getParquetUrls(
      "https://stac.example.com/catalog.json", "buildings", "building", Bounds.WORLD, CONFIG);

    assertEquals(List.of("https://s3.example.com/part-a.parquet", "https://s3.example.com/part-b.parquet"), result);
  }

  /**
   * Falls back to azure asset URL when aws is absent.
   */
  @Test
  void testGetParquetUrlsFallsBackToAzure() {
    var stubs = Map.of(
      "https://stac.example.com/catalog.json", """
        {"latest": "2026-02-18.0", "links": [
          {"rel": "child", "href": "./2026-02-18.0/catalog.json", "latest": true}
        ]}""",
      "https://stac.example.com/2026-02-18.0/catalog.json", """
        {"links": [{"rel": "child", "href": "./buildings/catalog.json", "title": "buildings"}]}""",
      "https://stac.example.com/2026-02-18.0/buildings/catalog.json", """
        {"links": [{"rel": "child", "href": "./building/collection.json", "title": "building"}]}""",
      "https://stac.example.com/2026-02-18.0/buildings/building/collection.json", """
        {"links": [{"rel": "item", "href": "./item-azure.json"}]}""",
      "https://stac.example.com/2026-02-18.0/buildings/building/item-azure.json", """
        {"bbox": [-10.0, 35.0, 30.0, 70.0],
         "assets": {"azure": {"href": "https://azure.example.com/part-a.parquet"}}}"""
    );
    OvertureStac.fetcher = stubs::get;

    List<String> result = OvertureStac.getParquetUrls(
      "https://stac.example.com/catalog.json", "buildings", "building", Bounds.WORLD, CONFIG);

    assertEquals(List.of("https://azure.example.com/part-a.parquet"), result);
  }

  /**
   * Item with no aws or azure asset is skipped with a warning (no exception thrown).
   */
  @Test
  void testGetParquetUrlsSkipsItemWithNoAsset() {
    var stubs = Map.of(
      "https://stac.example.com/catalog.json", """
        {"latest": "2026-02-18.0", "links": [
          {"rel": "child", "href": "./2026-02-18.0/catalog.json", "latest": true}
        ]}""",
      "https://stac.example.com/2026-02-18.0/catalog.json", """
        {"links": [{"rel": "child", "href": "./buildings/catalog.json", "title": "buildings"}]}""",
      "https://stac.example.com/2026-02-18.0/buildings/catalog.json", """
        {"links": [{"rel": "child", "href": "./building/collection.json", "title": "building"}]}""",
      "https://stac.example.com/2026-02-18.0/buildings/building/collection.json", """
        {"links": [{"rel": "item", "href": "./item-no-asset.json"}]}""",
      "https://stac.example.com/2026-02-18.0/buildings/building/item-no-asset.json", """
        {"bbox": [-10.0, 35.0, 30.0, 70.0], "assets": {}}"""
    );
    OvertureStac.fetcher = stubs::get;

    List<String> result = OvertureStac.getParquetUrls(
      "https://stac.example.com/catalog.json", "buildings", "building", Bounds.WORLD, CONFIG);

    assertEquals(List.of(), result);
  }

  /**
   * Unknown theme throws IllegalArgumentException.
   */
  @Test
  void testUnknownThemeThrows() {
    var stubs = Map.of(
      "https://stac.example.com/catalog.json", """
        {"latest": "2026-02-18.0", "links": [
          {"rel": "child", "href": "./2026-02-18.0/catalog.json", "latest": true}
        ]}""",
      "https://stac.example.com/2026-02-18.0/catalog.json", """
        {"links": [{"rel": "child", "href": "./buildings/catalog.json", "title": "buildings"}]}"""
    );
    OvertureStac.fetcher = stubs::get;

    assertThrows(IllegalArgumentException.class, () ->
      OvertureStac.getParquetUrls(
        "https://stac.example.com/catalog.json", "nonexistent", "building", Bounds.WORLD, CONFIG));
  }

  /**
   * Unknown type within a known theme throws IllegalArgumentException.
   */
  @Test
  void testUnknownTypeThrows() {
    var stubs = Map.of(
      "https://stac.example.com/catalog.json", """
        {"latest": "2026-02-18.0", "links": [
          {"rel": "child", "href": "./2026-02-18.0/catalog.json", "latest": true}
        ]}""",
      "https://stac.example.com/2026-02-18.0/catalog.json", """
        {"links": [{"rel": "child", "href": "./buildings/catalog.json", "title": "buildings"}]}""",
      "https://stac.example.com/2026-02-18.0/buildings/catalog.json", """
        {"links": [{"rel": "child", "href": "./building/collection.json", "title": "building"}]}"""
    );
    OvertureStac.fetcher = stubs::get;

    assertThrows(IllegalArgumentException.class, () ->
      OvertureStac.getParquetUrls(
        "https://stac.example.com/catalog.json", "buildings", "nonexistent", Bounds.WORLD, CONFIG));
  }
}
