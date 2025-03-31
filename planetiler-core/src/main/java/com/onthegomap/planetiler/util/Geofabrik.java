package com.onthegomap.planetiler.util;

import com.fasterxml.jackson.annotation.JsonProperty;
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
import net.jcip.annotations.Immutable;
import net.jcip.annotations.ThreadSafe;

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
   * If an exact match is found, returns that. Otherwise, looks for a resource that contains {@code searchQuery} as a
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

  private static synchronized IndexJson getAndCacheIndex(PlanetilerConfig config) {
    if (index == null) {
      try (
        InputStream inputStream = Downloader.openStream("https://download.geofabrik.de/index-v1-nogeom.json",
          config)
      ) {
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
    List<PropertiesJson> approxName = new ArrayList<>();
    List<PropertiesJson> id = new ArrayList<>();
    List<PropertiesJson> exactName = new ArrayList<>();
    for (var feature : index.features) {
      PropertiesJson properties = feature.properties;
      if (properties.urls.containsKey("pbf")) {
        if (properties.ids().stream().map(Geofabrik::tokenize).anyMatch(searchTokens::equals)) {
          id.add(properties);
        } else if (tokenize(properties.name).equals(searchTokens)) {
          exactName.add(properties);
        } else if (tokenize(properties.name).containsAll(searchTokens)) {
          approxName.add(properties);
        }
      }
    }
    String result = getIfOnly(searchQuery, "exact ID matches", id);
    if (result == null) {
      result = getIfOnly(searchQuery, "exact name matches", exactName);
    }
    if (result == null) {
      result = getIfOnly(searchQuery, "approximate name matches", approxName);
    }
    if (result == null) {
      throw new IllegalArgumentException("No matches for '" + searchQuery + "'");
    }
    return result;
  }

  private static String getIfOnly(String name, String searchQuery, List<PropertiesJson> values) {
    if (values.size() > 1) {
      throw new IllegalArgumentException(
        "Multiple " + name + " for '" + searchQuery + "': " + values.stream().map(d -> d.id).collect(
          Collectors.joining(", ")));
    } else if (values.size() == 1) {
      return values.getFirst().urls.get("pbf");
    } else {
      return null;
    }
  }

  record PropertiesJson(String id, String parent, String name, Map<String, String> urls,
    @JsonProperty("iso3166-1:alpha2") List<String> iso3166_1,
    @JsonProperty("iso3166-2") List<String> iso3166_2
  ) {
    List<String> ids() {
      List<String> result = new ArrayList<>(List.of(id, name));
      if (iso3166_1 != null) {
        result.addAll(iso3166_1);
      }
      if (iso3166_2 != null) {
        result.addAll(iso3166_2);
      }
      return result;
    }
  }

  record FeatureJson(PropertiesJson properties) {}

  @Immutable
  record IndexJson(List<FeatureJson> features) {}
}
