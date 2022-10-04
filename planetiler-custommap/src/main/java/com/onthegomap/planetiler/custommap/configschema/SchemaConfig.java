package com.onthegomap.planetiler.custommap.configschema;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.onthegomap.planetiler.custommap.YAML;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Map;

/**
 * An object representation of a vector tile server schema. This object is mapped to a schema YML file using SnakeYAML.
 */
public record SchemaConfig(
  @JsonProperty("schema_name") String schemaName,
  @JsonProperty("schema_description") String schemaDescription,
  String attribution,
  Map<String, DataSource> sources,
  Object definitions,
  @JsonProperty("tag_mappings") Map<String, Object> inputMappings,
  Collection<FeatureLayer> layers,
  Object examples,
  Map<String, Object> args
) {

  private static final String DEFAULT_ATTRIBUTION = """
    <a href="https://www.openstreetmap.org/copyright" target="_blank">&copy; OpenStreetMap contributors</a>
    """.trim();

  @Override
  public String attribution() {
    return attribution == null ? DEFAULT_ATTRIBUTION : attribution;
  }

  @Override
  public Map<String, Object> args() {
    return args == null ? Map.of() : args;
  }

  public static SchemaConfig load(Path path) {
    return YAML.load(path, SchemaConfig.class);
  }

  public static SchemaConfig load(String string) {
    return YAML.load(string, SchemaConfig.class);
  }
}
