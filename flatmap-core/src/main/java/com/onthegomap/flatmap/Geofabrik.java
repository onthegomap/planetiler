package com.onthegomap.flatmap;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * A utility for searching https://download.geofabrik.de/ for a .osm.pbf download URL.
 */
public class Geofabrik {

  private static final ObjectMapper objectMapper = new ObjectMapper()
    .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

  private static record Properties(String id, String parent, String name, Map<String, String> urls) {}

  private static record Feature(Properties properties) {}

  private static record Index(List<Feature> features) {}

  private static Set<String> tokens(String in) {
    return Stream.of(in.toLowerCase(Locale.ROOT).split("[^a-z]+")).collect(Collectors.toSet());
  }

  static String getDownloadUrl(String searchQuery, InputStream indexContent) throws IOException {
    Set<String> searchTokens = tokens(searchQuery);
    Index index = objectMapper.readValue(indexContent, Index.class);
    List<Properties> approx = new ArrayList<>();
    List<Properties> exact = new ArrayList<>();
    for (var feature : index.features) {
      Properties properties = feature.properties;
      if (properties.urls.containsKey("pbf")) {
        if (tokens(properties.id).equals(searchTokens) ||
          tokens(properties.name).equals(searchTokens)) {
          exact.add(properties);
        } else if (tokens(properties.name).containsAll(searchTokens)) {
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

  public static String getDownloadUrl(String searchQuery) {
    try (InputStream inputStream = new URL("https://download.geofabrik.de/index-v1-nogeom.json").openStream()) {
      return getDownloadUrl(searchQuery, inputStream);
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
  }
}
