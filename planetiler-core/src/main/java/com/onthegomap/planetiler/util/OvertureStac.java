package com.onthegomap.planetiler.util;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.onthegomap.planetiler.config.Bounds;
import com.onthegomap.planetiler.config.PlanetilerConfig;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.function.UnaryOperator;
import org.locationtech.jts.geom.Envelope;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Queries the <a href="https://stac.overturemaps.org/">Overture STAC catalog</a> to find the parquet file URLs for a
 * given theme/type, filtered to only files whose bbox intersects the requested bounds.
 *
 * <p>This avoids downloading the full Overture dataset when only a small geographic area is needed. The caller gets
 * back a list of HTTPS URLs that can be handed straight to {@link Downloader}.
 *
 * <p>Streaming (reading row groups over HTTP without saving to disk) would plug in here instead of returning URLs — see
 * the discussion in the feature issue for trade-offs.
 */
public class OvertureStac {

  private static final Logger LOGGER = LoggerFactory.getLogger(OvertureStac.class);

  // Base URL for the Overture STAC catalog. Exposed for testing so tests can point at a local server.
  static final String DEFAULT_CATALOG_URL = "https://stac.overturemaps.org/catalog.json";

  private static final ObjectMapper MAPPER = new ObjectMapper();

  // Swappable fetcher: tests replace this with a map lookup to avoid real HTTP calls.
  static UnaryOperator<String> fetcher = null;

  private OvertureStac() {}

  /**
   * Returns HTTPS URLs of parquet files for {@code theme}/{@code type} in the latest Overture release, keeping only
   * files whose STAC item bbox intersects {@code bounds}.
   */
  public static List<String> getParquetUrls(String theme, String type, Bounds bounds, PlanetilerConfig config) {
    return getParquetUrls(DEFAULT_CATALOG_URL, theme, type, bounds, config);
  }

  static List<String> getParquetUrls(String catalogUrl, String theme, String type, Bounds bounds,
    PlanetilerConfig config) {
    LOGGER.info("Fetching Overture STAC catalog from {}", catalogUrl);
    JsonNode catalog = fetch(catalogUrl, config);

    String latestCatalogUrl = resolveLatestCatalogUrl(catalog, catalogUrl);
    LOGGER.info("Using Overture release catalog: {}", latestCatalogUrl);
    JsonNode releaseCatalog = fetch(latestCatalogUrl, config);

    String themeCatalogUrl = resolveChildUrl(releaseCatalog, latestCatalogUrl, theme);
    if (themeCatalogUrl == null) {
      throw new IllegalArgumentException(
        "Overture theme '" + theme + "' not found in catalog " + latestCatalogUrl);
    }
    JsonNode themeCatalog = fetch(themeCatalogUrl, config);

    String collectionUrl = resolveChildUrl(themeCatalog, themeCatalogUrl, type);
    if (collectionUrl == null) {
      throw new IllegalArgumentException(
        "Overture type '" + type + "' not found in theme '" + theme + "' catalog " + themeCatalogUrl);
    }
    JsonNode collection = fetch(collectionUrl, config);

    Envelope latLonBounds = bounds.isWorld() ? null : bounds.latLon();
    List<String> urls = new ArrayList<>();
    for (JsonNode link : collection.path("links")) {
      if ("item".equals(link.path("rel").asText())) {
        String itemUrl = resolveUrl(collectionUrl, link.path("href").asText());
        JsonNode item = fetch(itemUrl, config);
        if (latLonBounds == null || itemBboxIntersects(item, latLonBounds)) {
          String parquetUrl = getAssetHref(item, "aws");
          if (parquetUrl == null) {
            parquetUrl = getAssetHref(item, "azure");
          }
          if (parquetUrl == null) {
            LOGGER.warn("No parquet asset found in STAC item {}", itemUrl);
          } else {
            urls.add(parquetUrl);
          }
        }
      }
    }

    LOGGER.info("Found {} parquet files for theme={} type={} within bounds", urls.size(), theme, type);
    return urls;
  }


  static String resolveLatestCatalogUrl(JsonNode catalog, String baseUrl) {
    for (JsonNode link : catalog.path("links")) {
      if ("child".equals(link.path("rel").asText()) && link.path("latest").asBoolean(false)) {
        return resolveUrl(baseUrl, link.path("href").asText());
      }
    }
    String latestVersion = catalog.path("latest").asText(null);
    if (latestVersion != null) {
      return resolveUrl(baseUrl, "./" + latestVersion + "/catalog.json");
    }
    throw new IllegalArgumentException("Could not find latest Overture release in catalog " + baseUrl);
  }


  static String resolveChildUrl(JsonNode catalog, String baseUrl, String name) {
    for (JsonNode link : catalog.path("links")) {
      if (!"child".equals(link.path("rel").asText())) {
        continue;
      }
      String title = link.path("title").asText("");
      String href = link.path("href").asText("");
      String segment = hrefSegment(href);
      if (name.equalsIgnoreCase(title) || name.equalsIgnoreCase(segment)) {
        return resolveUrl(baseUrl, href);
      }
    }
    return null;
  }

  /** Returns the first meaningful path segment from a relative href like {@code "./buildings/catalog.json"}. */
  private static String hrefSegment(String href) {
    String stripped = href.startsWith("./") ? href.substring(2) : href;
    int slash = stripped.indexOf('/');
    return slash < 0 ? stripped : stripped.substring(0, slash);
  }

  /** Returns true if the STAC item's bbox overlaps {@code filter}. */
  static boolean itemBboxIntersects(JsonNode item, Envelope filter) {
    JsonNode bbox = item.path("bbox");
    if (!bbox.isArray() || bbox.size() < 4) {
      return true;
    }
    double minLon = bbox.get(0).asDouble();
    double minLat = bbox.get(1).asDouble();
    double maxLon = bbox.get(2).asDouble();
    double maxLat = bbox.get(3).asDouble();
    return filter.intersects(new Envelope(minLon, maxLon, minLat, maxLat));
  }


  private static String getAssetHref(JsonNode item, String assetKey) {
    JsonNode asset = item.path("assets").path(assetKey);
    if (asset.isMissingNode()) {
      return null;
    }
    String href = asset.path("href").asText(null);
    return (href == null || href.isBlank()) ? null : href;
  }


  static String resolveUrl(String base, String href) {
    if (href.startsWith("http://") || href.startsWith("https://")) {
      return href;
    }
    return URI.create(base).resolve(href).toString();
  }

  static JsonNode fetch(String url, PlanetilerConfig config) {
    // In tests, fetcher is set to a map lookup so no real HTTP is made.
    if (fetcher != null) {
      String json = fetcher.apply(url);
      if (json == null) {
        throw new IllegalStateException("No stub for URL: " + url);
      }
      try {
        return MAPPER.readTree(json);
      } catch (IOException e) {
        throw new IllegalStateException("Bad stub JSON for URL: " + url, e);
      }
    }
    try (InputStream in = Downloader.openStream(url, config)) {
      return MAPPER.readTree(in);
    } catch (IOException e) {
      throw new IllegalStateException("Failed to fetch STAC URL: " + url, e);
    }
  }
}
