package com.onthegomap.planetiler.util;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.onthegomap.planetiler.config.PlanetilerConfig;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.concurrent.ThreadSafe;

/**
 * A utility to search <a href="https://download.geofabrik.de/">Geofabrik Download Server</a> for a {@code .osm.pbf}
 * download URL by name.
 *
 * @see <a href="https://download.geofabrik.de/technical.html">Geofabrik JSON index technical details</a>
 */
@ThreadSafe
public class Geofabrik {

  private static volatile IndexJson index = null;
  private static final ObjectMapper objectMapper = new ObjectMapper()
    .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

  /**
   * Fetches the Geofabrik index and searches for a {@code .osm.pbf} resource to download where ID or name field
   * contains all the tokens in {@code searchQuery}.
   * <p>
   * If an exact match is found, returns that. Otherwise, looks for a resource that contains  {@code searchQuery} as a
   * substring.
   * <p>
   * The index is only fetched once and cached after that.
   *
   * @param searchQuery the tokens to search for
   * @param config      planetiler config with user-agent and timeout to use when downloading files
   * @return the URL of a {@code .osm.pbf} file with name or ID matching {@code searchQuery}
   * @throws IllegalArgumentException if no matches, or more than one match is found.
   */
  public static String getDownloadUrl(String searchQuery, PlanetilerConfig config) {
    IndexJson index = getAndCacheIndex(config);
    return searchIndexForDownloadUrl(searchQuery, index);
  }

  private synchronized static IndexJson getAndCacheIndex(PlanetilerConfig config) {
    if (index == null) {
      try (InputStream inputStream = Downloader.openStream("https://download.geofabrik.de/index-v1-nogeom.json",
        config)) {
        index = parseIndexJson(inputStream);
      } catch (IOException e) {
        throw new IllegalStateException(e);
      }
    }
    return index;
  }

  private static Set<String> tokenize(String in) {
    return Stream.of(in.toLowerCase(Locale.ROOT).split("[^a-z]+")).collect(Collectors.toSet());
  }

  static IndexJson parseIndexJson(InputStream indexJsonContent) throws IOException {
    return objectMapper.readValue(indexJsonContent, IndexJson.class);
  }

  static String searchIndexForDownloadUrl(String searchQuery, IndexJson index) {
    Set<String> searchTokens = tokenize(searchQuery);
    List<PropertiesJson> approx = new ArrayList<>();
    List<PropertiesJson> exact = new ArrayList<>();
    for (var feature : index.features) {
      PropertiesJson properties = feature.properties;
      if (properties.urls.containsKey("pbf")) {
        if (tokenize(properties.id).equals(searchTokens) ||
          tokenize(properties.name).equals(searchTokens)) {
          exact.add(properties);
        } else if (tokenize(properties.name).containsAll(searchTokens)) {
          approx.add(properties);
        }
      }
    }
    if (exact.size() > 1) {
      throw new IllegalArgumentException(
        "Multiple exact matches for '" + searchQuery + "': " + exact.stream().map(d -> d.id).collect(
          Collectors.joining(", ")));
    } else if (exact.size() == 1) {
      return exact.get(0).urls.get("pbf");
    } else {
      if (approx.size() > 1) {
        throw new IllegalArgumentException(
          "Multiple approximate matches for '" + searchQuery + "': " + approx.stream().map(d -> d.id).collect(
            Collectors.joining(", ")));
      } else if (approx.size() == 1) {
        return approx.get(0).urls.get("pbf");
      } else {
        throw new IllegalArgumentException("No matches for '" + searchQuery + "'");
      }
    }
  }

  static record PropertiesJson(String id, String parent, String name, Map<String, String> urls) {}

  static record FeatureJson(PropertiesJson properties) {}

  static record IndexJson(List<FeatureJson> features) {}
}
