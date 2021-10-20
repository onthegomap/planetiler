package com.onthegomap.flatmap.util;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlElementWrapper;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.onthegomap.flatmap.config.FlatmapConfig;
import java.io.IOException;
import java.io.InputStream;
import java.util.Comparator;
import java.util.List;

/**
 * A utility to download {@code planet.osm.pbf} files from <a href="https://registry.opendata.aws/osm/">AWS Open Data
 * Registry</a>.
 */
public class AwsOsm {

  private static final String BASE = "https://osm-pds.s3.amazonaws.com/";
  private static volatile IndexXml index = null;
  private static final ObjectMapper mapper = new XmlMapper().registerModule(new Jdk8Module());

  /**
   * Fetches the AWS Open Data Registry index and searches for a {@code .osm.pbf} resource to download where snapshot
   * date matches {@code searchQuery}, or the latest snapshot if {@code searchQuery == "latest"}.
   * <p>
   * The index is only fetched once and cached after that.
   *
   * @param searchQuery the snapshot to search for
   * @param config      flatmap config with user-agent and timeout to use when downloading
   * @return the URL of a {@code .osm.pbf} file with name or snapshot ID matching {@code searchQuery}
   * @throws IllegalArgumentException if no matches, or more than one match is found.
   */
  public static String getDownloadUrl(String searchQuery, FlatmapConfig config) {
    IndexXml index = getAndCacheIndex(config);
    return searchIndexForDownloadUrl(searchQuery, index);
  }

  private synchronized static IndexXml getAndCacheIndex(FlatmapConfig config) {
    if (index == null) {
      try (InputStream inputStream = Downloader.openStream(BASE, config)) {
        index = parseIndexXml(inputStream);
      } catch (IOException e) {
        throw new IllegalStateException(e);
      }
    }
    return index;
  }

  static IndexXml parseIndexXml(InputStream indexXmlContent) throws IOException {
    return mapper.readValue(indexXmlContent, IndexXml.class);
  }

  static String searchIndexForDownloadUrl(String searchQuery, IndexXml index) {
    if ("latest".equalsIgnoreCase(searchQuery)) {
      return index.contents.stream()
        .filter(c -> c.key.endsWith(".osm.pbf"))
        .map(c -> BASE + c.key)
        .max(Comparator.naturalOrder())
        .orElseThrow(() -> new IllegalArgumentException("Unable to find latest AWS osm download URL"));
    } else {
      List<String> results = index.contents.stream()
        .filter(c -> c.key.endsWith("/planet-" + searchQuery + ".osm.pbf"))
        .map(c -> BASE + c.key)
        .toList();
      if (results.isEmpty()) {
        throw new IllegalArgumentException("Unable to find AWS osm download URL for " + searchQuery);
      } else if (results.size() > 1) {
        throw new IllegalArgumentException("Found multiple AWS osm download URLs for " + searchQuery + ": " + results);
      }
      return results.get(0);
    }
  }

  @JsonIgnoreProperties(ignoreUnknown = true)
  record IndexXml(
    @JacksonXmlProperty(localName = "Contents")
    @JacksonXmlElementWrapper(useWrapping = false)
    List<ContentXml> contents
  ) {}

  @JsonIgnoreProperties(ignoreUnknown = true)
  record ContentXml(
    @JacksonXmlProperty(localName = "Key")
    String key
  ) {}
}
