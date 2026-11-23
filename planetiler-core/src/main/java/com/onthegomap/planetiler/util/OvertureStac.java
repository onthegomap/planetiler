package com.onthegomap.planetiler.util;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.onthegomap.planetiler.config.Bounds;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import org.locationtech.jts.geom.Envelope;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Queries the <a href="https://stac.overturemaps.org/">Overture STAC catalog</a> to find the parquet file URLs for a
 * given theme/type, filtered to only files whose bbox intersects the requested bounds.
 *
 * <p>
 * This avoids downloading the full Overture dataset when only a small geographic area is needed. The caller gets back a
 * list of HTTPS URLs that can be handed straight to {@link Downloader}.
 *
 * <p>
 * Streaming (reading row groups over HTTP without saving to disk) would plug in here instead of returning URLs — see
 * the discussion in the feature issue for trade-offs.
 */
public class OvertureStac {

  private static final Logger LOGGER = LoggerFactory.getLogger(OvertureStac.class);

  /** The URL of the official Overture STAC catalog. */
  public static final String DEFAULT_CATALOG_URL = "https://stac.overturemaps.org/catalog.json";

  private static final ObjectMapper MAPPER =
    new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

  private final Downloader downloader;

  /** Creates an {@link OvertureStac} that fetches catalogs through {@code downloader}. */
  public OvertureStac(Downloader downloader) {
    this.downloader = downloader;
  }


  // Java records for STAC JSON deserialization


  record StacLink(String rel, String href, String title, boolean latest) {}

  record StacCatalog(String latest, List<StacLink> links) {
    StacCatalog {
      if (links == null) {
        links = List.of();
      }
    }
  }

  record StacCollection(List<StacLink> links) {
    StacCollection {
      if (links == null) {
        links = List.of();
      }
    }
  }

  record StacAsset(String href) {}

  record SavedList(String catalogUrl, String theme, String type, List<ParquetFile> files) {}

  record StacItem(List<Double> bbox, Map<String, StacAsset> assets) {
    StacItem {
      if (assets == null) {
        assets = Map.of();
      }
    }
  }


  // Public API


  /**
   * A parquet file in an Overture release.
   *
   * @param url  where to read the file from
   * @param bbox {@code [minLon, minLat, maxLon, maxLat]} of the features in the file, or null if the catalog doesn't
   *             say
   */
  public record ParquetFile(String url, List<Double> bbox) {

    boolean intersects(Envelope latLonBounds) {
      if (bbox == null || bbox.size() < 4) {
        return true; // no bbox → include conservatively
      }
      return latLonBounds.intersects(new Envelope(bbox.get(0), bbox.get(2), bbox.get(1), bbox.get(3)));
    }
  }

  /**
   * Returns HTTPS URLs of parquet files for {@code theme}/{@code type} in the latest Overture release, keeping only
   * files whose STAC item bbox intersects {@code bounds}.
   */
  public List<String> getParquetUrls(String theme, String type, Bounds bounds) {
    return getParquetUrls(DEFAULT_CATALOG_URL, theme, type, bounds);
  }

  /**
   * Same as {@link #getParquetUrls(String, String, String, Bounds)}, but saves the list of every file in the release
   * for {@code theme}/{@code type} in {@code savedList} and filters it from there in later lookups with the same
   * catalog, theme and type (for any bounds) unless {@code refresh}, so reruns need no requests.
   */
  public List<String> getParquetUrls(String catalogUrl, String theme, String type, Bounds bounds, Path savedList,
    boolean refresh) {
    return inBounds(listParquetFiles(catalogUrl, theme, type, savedList, refresh), theme, type, bounds);
  }

  /** Same as {@link #getParquetUrls(String, String, Bounds)} but using the STAC catalog at {@code catalogUrl}. */
  public List<String> getParquetUrls(String catalogUrl, String theme, String type, Bounds bounds) {
    return inBounds(listParquetFiles(catalogUrl, theme, type), theme, type, bounds);
  }

  private List<ParquetFile> listParquetFiles(String catalogUrl, String theme, String type, Path savedList,
    boolean refresh) {
    if (!refresh && Files.exists(savedList)) {
      try {
        var saved = MAPPER.readValue(savedList.toFile(), SavedList.class);
        if (saved.equals(new SavedList(catalogUrl, theme, type, saved.files()))) {
          // Overture removes old releases after a while, so say how to move to the latest one.
          LOGGER.info("Using the list of {}/{} files an earlier run saved in {}, run with --refresh-sources to look " +
            "up the latest Overture release", theme, type, savedList);
          return saved.files();
        }
      } catch (IOException e) {
        LOGGER.warn("Ignoring unreadable {}: {}", savedList, e.toString());
      }
    }
    var files = listParquetFiles(catalogUrl, theme, type);
    try {
      FileUtils.createParentDirectories(savedList);
      MAPPER.writeValue(savedList.toFile(), new SavedList(catalogUrl, theme, type, files));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    return files;
  }

  private static List<String> inBounds(List<ParquetFile> files, String theme, String type, Bounds bounds) {
    var urls = files.stream()
      .filter(file -> bounds.isWorld() || file.intersects(bounds.latLon()))
      .map(ParquetFile::url)
      .toList();
    LOGGER.info("Found {} parquet files for theme={} type={} within bounds", urls.size(), theme, type);
    return urls;
  }

  /**
   * Returns every parquet file for {@code theme}/{@code type} in the latest Overture release of the STAC catalog at
   * {@code catalogUrl}, in catalog order.
   */
  public List<ParquetFile> listParquetFiles(String catalogUrl, String theme, String type) {
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

    List<String> itemUrls = new ArrayList<>();
    for (StacLink link : collection.links()) {
      if ("item".equals(link.rel())) {
        itemUrls.add(resolveUrl(collectionUrl, link.href()));
      }
    }

    // Fetch all items in parallel on virtual threads to avoid blocking the fork/join pool.
    List<CompletableFuture<ParquetFile>> futures;
    try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
      futures = itemUrls.stream()
        .map(itemUrl -> CompletableFuture.supplyAsync(() -> {
          StacItem item = fetch(itemUrl, StacItem.class);
          // Prefer AWS, fall back to Azure.
          String parquetUrl = getAssetHref(item, "aws");
          if (parquetUrl == null) {
            parquetUrl = getAssetHref(item, "azure");
          }
          if (parquetUrl == null) {
            LOGGER.warn("No parquet asset found in STAC item {}", itemUrl);
            return null;
          }
          return new ParquetFile(parquetUrl, item.bbox());
        }, executor))
        .toList();
    }

    List<ParquetFile> files = new ArrayList<>();
    for (CompletableFuture<ParquetFile> future : futures) {
      ParquetFile file = future.join();
      if (file != null) {
        files.add(file);
      }
    }
    return files;
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

  private <T> T fetch(String url, Class<T> type) {
    try (InputStream in = downloader.openStream(url)) {
      return MAPPER.readValue(in, type);
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to fetch STAC URL: " + url, e);
    }
  }
}
