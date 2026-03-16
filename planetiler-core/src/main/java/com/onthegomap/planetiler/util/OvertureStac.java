package com.onthegomap.planetiler.util;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.onthegomap.planetiler.config.Bounds;
import com.onthegomap.planetiler.config.PlanetilerConfig;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
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

  static final String DEFAULT_CATALOG_URL = "https://stac.overturemaps.org/catalog.json";

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private final PlanetilerConfig config;

  public OvertureStac(PlanetilerConfig config) {
    this.config = config;
  }

  /** Creates an {@link OvertureStac} backed by real HTTP using the default Overture catalog URL. */
  public static OvertureStac create(PlanetilerConfig config) {
    return new OvertureStac(config);
  }

  
  // Java records for STAC JSON deserialization
  

  @JsonIgnoreProperties(ignoreUnknown = true)
  record StacLink(String rel, String href, String title, boolean latest) {}

  @JsonIgnoreProperties(ignoreUnknown = true)
  record StacCatalog(String latest, List<StacLink> links) {
    StacCatalog {
      if (links == null) {
        links = List.of();
      }
    }
  }

  @JsonIgnoreProperties(ignoreUnknown = true)
  record SpatialExtent(List<List<Double>> bbox) {}

  @JsonIgnoreProperties(ignoreUnknown = true)
  record StacExtent(SpatialExtent spatial) {}

  @JsonIgnoreProperties(ignoreUnknown = true)
  record StacCollection(List<StacLink> links, StacExtent extent) {
    StacCollection {
      if (links == null) {
        links = List.of();
      }
    }
  }

  @JsonIgnoreProperties(ignoreUnknown = true)
  record StacAsset(String href) {}

  @JsonIgnoreProperties(ignoreUnknown = true)
  record StacItem(List<Double> bbox, Map<String, StacAsset> assets) {
    StacItem {
      if (assets == null) {
        assets = Map.of();
      }
    }
  }

  
  // Public API
  

  /**
   * Returns HTTPS URLs of parquet files for {@code theme}/{@code type} in the latest Overture release, keeping only
   * files whose STAC item bbox intersects {@code bounds}.
   */
  public List<String> getParquetUrls(String theme, String type, Bounds bounds) {
    return getParquetUrls(DEFAULT_CATALOG_URL, theme, type, bounds);
  }

  List<String> getParquetUrls(String catalogUrl, String theme, String type, Bounds bounds) {
    LOGGER.info("Fetching Overture STAC catalog from {}", catalogUrl);
    StacCatalog catalog = fetch(catalogUrl, StacCatalog.class);

    String latestCatalogUrl = resolveLatestCatalogUrl(catalog, catalogUrl);
    LOGGER.info("Using Overture release catalog: {}", latestCatalogUrl);
    StacCatalog releaseCatalog = fetch(latestCatalogUrl, StacCatalog.class);

    String themeCatalogUrl = resolveChildUrl(releaseCatalog, latestCatalogUrl, theme);
    if (themeCatalogUrl == null) {
      throw new IllegalArgumentException(
        "Overture theme '" + theme + "' not found in catalog " + latestCatalogUrl);
    }
    StacCatalog themeCatalog = fetch(themeCatalogUrl, StacCatalog.class);

    String collectionUrl = resolveChildUrl(themeCatalog, themeCatalogUrl, type);
    if (collectionUrl == null) {
      throw new IllegalArgumentException(
        "Overture type '" + type + "' not found in theme '" + theme + "' catalog " + themeCatalogUrl);
    }
    StacCollection collection = fetch(collectionUrl, StacCollection.class);

    Envelope latLonBounds = bounds.isWorld() ? null : bounds.latLon();

    // Skip the whole collection if its declared spatial extent doesn't intersect our bounds.
    if (latLonBounds != null && !collectionExtentIntersects(collection, latLonBounds)) {
      LOGGER.debug("Skipping collection {} — extent does not intersect bounds", collectionUrl);
      return List.of();
    }

    // Gather item links; per-item bbox check happens after fetching each item below.
    List<String> itemUrls = new ArrayList<>();
    for (StacLink link : collection.links()) {
      if ("item".equals(link.rel())) {
        String itemUrl = resolveUrl(collectionUrl, link.href());
        itemUrls.add(itemUrl);
      }
    }

    // Fetch all items in parallel, then extract parquet URLs.
    List<CompletableFuture<String>> futures = itemUrls.stream()
      .map(itemUrl -> CompletableFuture.supplyAsync(() -> {
        StacItem item = fetch(itemUrl, StacItem.class);
        if (latLonBounds != null && !itemBboxIntersects(item, latLonBounds)) {
          return null;
        }
        // Prefer AWS, fall back to Azure.
        String parquetUrl = getAssetHref(item, "aws");
        if (parquetUrl == null) {
          parquetUrl = getAssetHref(item, "azure");
        }
        if (parquetUrl == null) {
          LOGGER.warn("No parquet asset found in STAC item {}", itemUrl);
        }
        return parquetUrl;
      }))
      .toList();

    List<String> urls = new ArrayList<>();
    for (CompletableFuture<String> future : futures) {
      try {
        String url = future.get();
        if (url != null) {
          urls.add(url);
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IllegalStateException("Interrupted while fetching STAC items", e);
      } catch (ExecutionException e) {
        throw new IllegalStateException("Failed to fetch STAC item", e.getCause());
      }
    }

    LOGGER.info("Found {} parquet files for theme={} type={} within bounds", urls.size(), theme, type);
    return urls;
  }

  
  // Helpers
  

  String resolveLatestCatalogUrl(StacCatalog catalog, String baseUrl) {
    for (StacLink link : catalog.links()) {
      if ("child".equals(link.rel()) && link.latest()) {
        return resolveUrl(baseUrl, link.href());
      }
    }
    String latestVersion = catalog.latest();
    if (latestVersion != null) {
      return resolveUrl(baseUrl, "./" + latestVersion + "/catalog.json");
    }
    throw new IllegalArgumentException("Could not find latest Overture release in catalog " + baseUrl);
  }

  String resolveChildUrl(StacCatalog catalog, String baseUrl, String name) {
    for (StacLink link : catalog.links()) {
      if (!"child".equals(link.rel())) {
        continue;
      }
      String href = link.href() == null ? "" : link.href();
      String title = link.title() == null ? "" : link.title();
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

  /** Returns false only if the collection's declared extent bbox is known and does not overlap {@code filter}. */
  private static boolean collectionExtentIntersects(StacCollection collection, Envelope filter) {
    if (collection.extent() == null || collection.extent().spatial() == null) {
      return true;
    }
    List<List<Double>> bboxes = collection.extent().spatial().bbox();
    if (bboxes == null || bboxes.isEmpty()) {
      return true;
    }
    // STAC extent.spatial.bbox is a list of bboxes; the first entry is the overall union.
    List<Double> bbox = bboxes.get(0);
    if (bbox == null || bbox.size() < 4) {
      return true;
    }
    double minLon = bbox.get(0);
    double minLat = bbox.get(1);
    double maxLon = bbox.get(2);
    double maxLat = bbox.get(3);
    return filter.intersects(new Envelope(minLon, maxLon, minLat, maxLat));
  }

  /** Returns true if the STAC item's bbox overlaps {@code filter}. */
  static boolean itemBboxIntersects(StacItem item, Envelope filter) {
    List<Double> bbox = item.bbox();
    if (bbox == null || bbox.size() < 4) {
      return true; // no bbox → include conservatively
    }
    double minLon = bbox.get(0);
    double minLat = bbox.get(1);
    double maxLon = bbox.get(2);
    double maxLat = bbox.get(3);
    return filter.intersects(new Envelope(minLon, maxLon, minLat, maxLat));
  }

  private static String getAssetHref(StacItem item, String assetKey) {
    StacAsset asset = item.assets().get(assetKey);
    if (asset == null) {
      return null;
    }
    String href = asset.href();
    return (href == null || href.isBlank()) ? null : href;
  }

  static String resolveUrl(String base, String href) {
    if (href.startsWith("http://") || href.startsWith("https://")) {
      return href;
    }
    return URI.create(base).resolve(href).toString();
  }

  /**
   * Fetches and deserializes a STAC JSON resource. Subclasses can override to provide stub responses in tests (see
   * {@link Downloader} for the same pattern).
   */
  protected <T> T fetch(String url, Class<T> type) {
    try (InputStream in = Downloader.openStream(url, config)) {
      return MAPPER.readValue(in, type);
    } catch (IOException e) {
      throw new IllegalStateException("Failed to fetch STAC URL: " + url, e);
    }
  }
}
