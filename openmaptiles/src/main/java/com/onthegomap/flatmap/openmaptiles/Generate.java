package com.onthegomap.flatmap.openmaptiles;

import com.fasterxml.jackson.annotation.JsonIdentityInfo;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.ObjectIdGenerators;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.onthegomap.flatmap.Arguments;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Generate {

  private static Logger LOGGER = LoggerFactory.getLogger(Generate.class);

  private static record OpenmaptilesConfig(
    OpenmaptilesTileSet tileset
  ) {}

  @JsonIgnoreProperties(ignoreUnknown = true)
  private static record OpenmaptilesTileSet(
    List<String> layers,
    String version,
    String attribution,
    List<String> languages
  ) {}

  @JsonIgnoreProperties(ignoreUnknown = true)
  private static record LayerDetails(
    String id,
    String requires,
    String description,
    Map<String, JsonNode> fields,
    double buffer_size
  ) {}

  private static record Datasource(
    String type,
    String mapping_file
  ) {}

  @JsonIgnoreProperties(ignoreUnknown = true)
  private static record LayerConfig(
    LayerDetails layer,
    List<Datasource> datasources
  ) {}

  @JsonIdentityInfo(generator = ObjectIdGenerators.PropertyGenerator.class,
    property = "columnId")
  private static record Imposm3Column(
    String columnId,
    String type,
    String name,
    String key
  ) {}

  private static record Imposm3Table(
    String type,
    List<Imposm3Column> columns,
    JsonNode mapping
  ) {}

  private static record Imposm3Mapping(
    Map<String, Imposm3Table> tables
  ) {}

  public static void main(String[] args) throws IOException, URISyntaxException {
    var objectMapper = new ObjectMapper(new YAMLFactory());
    Arguments arguments = Arguments.fromJvmProperties();
    String tag = arguments.get("tag", "openmaptiles tag to use", "v3.12.2");
    String base = "https://raw.githubusercontent.com/openmaptiles/openmaptiles/" + tag + "/";

    var rootUrl = new URL(base + "openmaptiles.yaml");
    LOGGER.info("reading " + rootUrl);
    OpenmaptilesConfig config = objectMapper.readValue(rootUrl, OpenmaptilesConfig.class);

    List<LayerConfig> layers = new ArrayList<>();
    Set<URI> mappingFiles = new LinkedHashSet<>();
    for (String layerFile : config.tileset.layers) {
      URL layerURL = new URL(base + layerFile);
      LOGGER.info("reading " + layerURL);
      LayerConfig layer = objectMapper.readValue(layerURL, LayerConfig.class);
      layers.add(layer);
      for (Datasource datasource : layer.datasources) {
        if ("imposm3".equals(datasource.type)) {
          mappingFiles.add(layerURL.toURI().resolve(datasource.mapping_file).normalize());
        } else {
          LOGGER.warn("Unknown datasource type: " + datasource.type);
        }
      }
    }

    for (URI uri : mappingFiles) {
      LOGGER.info("reading " + uri);
      Imposm3Mapping layer = objectMapper.readValue(uri.toURL(), Imposm3Mapping.class);
      LOGGER.info(layer.toString());
    }
  }
}
