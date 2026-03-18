package com.onthegomap.planetiler.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.onthegomap.planetiler.config.Bounds;
import com.onthegomap.planetiler.config.PlanetilerConfig;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.locationtech.jts.geom.Envelope;

/**
 * Tests for {@link OvertureStac} — all catalog JSON is provided inline; no real HTTP calls are made.
 * Uses a subclass that overrides {@link OvertureStac#fetch} to return stub responses from a map.
 */
class OvertureStacTest {

  private static final ObjectMapper MAPPER = new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
  private static final PlanetilerConfig CONFIG = PlanetilerConfig.defaults();

  /** OvertureStac subclass that serves stub JSON from a map instead of making real HTTP requests. */
  private static OvertureStac stubStac(Map<String, String> stubs) {
    return new OvertureStac(CONFIG) {
      @Override
      protected <T> T fetch(String url, Class<T> type) {
        String json = stubs.get(url);
        if (json == null) {
          throw new IllegalStateException("No stub for URL: " + url);
        }
        try {
          return MAPPER.readValue(json, type);
        } catch (IOException e) {
          throw new IllegalStateException("Bad stub JSON for URL: " + url, e);
        }
      }
    };
  }



  @Test
  void testLatestLinkByFlag() throws IOException {
    // The link with "latest":true should be preferred
    var catalog = MAPPER.readValue("""
      {
        "links": [
          {"rel": "child", "href": "./2026-01-01.0/catalog.json"},
          {"rel": "child", "href": "./2026-02-18.0/catalog.json", "latest": true}
        ]
      }
      """, OvertureStac.StacCatalog.class);
    String url = stubStac(Map.of()).resolveLatestCatalogUrl(catalog,
      "https://stac.overturemaps.org/catalog.json");
    assertEquals("https://stac.overturemaps.org/2026-02-18.0/catalog.json", url);
  }

  @Test
  void testLatestFallbackToTopLevelField() throws IOException {
    // No link has "latest":true — fall back to the top-level "latest" string field
    var catalog = MAPPER.readValue("""
      {
        "latest": "2026-02-18.0",
        "links": [
          {"rel": "child", "href": "./2026-01-01.0/catalog.json"},
          {"rel": "child", "href": "./2026-02-18.0/catalog.json"}
        ]
      }
      """, OvertureStac.StacCatalog.class);
    String url = stubStac(Map.of()).resolveLatestCatalogUrl(catalog,
      "https://stac.overturemaps.org/catalog.json");
    assertEquals("https://stac.overturemaps.org/2026-02-18.0/catalog.json", url);
  }

  @Test
  void testLatestNotFound() throws IOException {
    var catalog = MAPPER.readValue("""
      { "links": [{"rel": "root", "href": "./catalog.json"}] }
      """, OvertureStac.StacCatalog.class);
    assertThrows(IllegalArgumentException.class,
      () -> stubStac(Map.of()).resolveLatestCatalogUrl(catalog,
        "https://stac.overturemaps.org/catalog.json"));
  }


  @Test
  void testResolveChildByTitle() throws IOException {
    var catalog = MAPPER.readValue("""
      {
        "links": [
          {"rel": "child", "href": "./buildings/catalog.json", "title": "buildings"},
          {"rel": "child", "href": "./places/catalog.json",   "title": "places"}
        ]
      }
      """, OvertureStac.StacCatalog.class);
    String url = stubStac(Map.of()).resolveChildUrl(catalog,
      "https://stac.overturemaps.org/2026-02-18.0/catalog.json", "buildings");
    assertEquals("https://stac.overturemaps.org/2026-02-18.0/buildings/catalog.json", url);
  }

  @Test
  void testResolveChildByHrefSegment() throws IOException {
    // title is absent — match by the first path segment in the href
    var catalog = MAPPER.readValue("""
      {
        "links": [
          {"rel": "child", "href": "./transportation/catalog.json"},
          {"rel": "child", "href": "./base/catalog.json"}
        ]
      }
      """, OvertureStac.StacCatalog.class);
    String url = stubStac(Map.of()).resolveChildUrl(catalog,
      "https://stac.overturemaps.org/2026-02-18.0/catalog.json", "transportation");
    assertEquals("https://stac.overturemaps.org/2026-02-18.0/transportation/catalog.json", url);
  }

  @Test
  void testResolveChildNotFound() throws IOException {
    var catalog = MAPPER.readValue("""
      { "links": [{"rel": "child", "href": "./buildings/catalog.json", "title": "buildings"}] }
      """, OvertureStac.StacCatalog.class);
    assertEquals(null, stubStac(Map.of()).resolveChildUrl(catalog,
      "https://stac.overturemaps.org/2026-02-18.0/catalog.json", "places"));
  }


  @Test
  void testBboxIntersectsOverlap() throws IOException {
    // Item covers western hemisphere; filter covers Monaco area — no overlap
    var item = MAPPER.readValue("""
      {"bbox": [-179.9, -84.3, -2.8, -22.5]}
      """, OvertureStac.StacItem.class);
    Envelope monacoFilter = new Envelope(7.3, 7.5, 43.7, 43.8);
    assertFalse(OvertureStac.itemBboxIntersects(item, monacoFilter));
  }

  @Test
  void testBboxIntersectsMatch() throws IOException {
    // Item covers Europe; filter is Monaco — they intersect
    var item = MAPPER.readValue("""
      {"bbox": [-10.0, 35.0, 30.0, 70.0]}
      """, OvertureStac.StacItem.class);
    Envelope monacoFilter = new Envelope(7.3, 7.5, 43.7, 43.8);
    assertTrue(OvertureStac.itemBboxIntersects(item, monacoFilter));
  }

  @Test
  void testBboxMissingIsInclusive() throws IOException {
    // No bbox → include the item conservatively
    var item = MAPPER.readValue("""
      {"id": "some-item"}
      """, OvertureStac.StacItem.class);
    assertTrue(OvertureStac.itemBboxIntersects(item, new Envelope(7.3, 7.5, 43.7, 43.8)));
  }


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

  // getParquetUrls (stubbed HTTP)

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

    Bounds monaco = new Bounds(new Envelope(7.3, 7.5, 43.7, 43.8));
    List<String> result = stubStac(stubs).getParquetUrls(
      "https://stac.example.com/catalog.json", "buildings", "building", monaco);

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

    List<String> result = stubStac(stubs).getParquetUrls(
      "https://stac.example.com/catalog.json", "buildings", "building", Bounds.WORLD);

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

    List<String> result = stubStac(stubs).getParquetUrls(
      "https://stac.example.com/catalog.json", "buildings", "building", Bounds.WORLD);

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

    List<String> result = stubStac(stubs).getParquetUrls(
      "https://stac.example.com/catalog.json", "buildings", "building", Bounds.WORLD);

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

    assertThrows(IllegalArgumentException.class, () ->
      stubStac(stubs).getParquetUrls(
        "https://stac.example.com/catalog.json", "nonexistent", "building", Bounds.WORLD));
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

    assertThrows(IllegalArgumentException.class, () ->
      stubStac(stubs).getParquetUrls(
        "https://stac.example.com/catalog.json", "buildings", "nonexistent", Bounds.WORLD));
  }
}
