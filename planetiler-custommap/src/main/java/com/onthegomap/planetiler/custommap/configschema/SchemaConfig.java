package com.onthegomap.planetiler.custommap.configschema;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Collection;
import java.util.Map;

/**
 * An object representation of a vector tile server schema. This object is mapped to a schema YML file using SnakeYAML.
 */
public class SchemaConfig {
  private String schemaName;
  private String schemaDescription;
  private String attribution =
    "<a href=\\\"https://www.openstreetmap.org/copyright\\\" target=\\\"_blank\\\">&copy; OpenStreetMap contributors</a>";
  private Map<String, DataSource> sources;

  @JsonProperty("tag_mappings")
  private Map<String, Object> inputMappings;
  private Collection<FeatureLayer> layers;

  public String getSchemaName() {
    return schemaName;
  }

  public void setSchemaName(String schemaName) {
    this.schemaName = schemaName;
  }

  public String getSchemaDescription() {
    return schemaDescription;
  }

  public void setSchemaDescription(String schemaDescription) {
    this.schemaDescription = schemaDescription;
  }

  public String getAttribution() {
    return attribution;
  }

  public void setAttribution(String attribution) {
    this.attribution = attribution;
  }

  public Map<String, DataSource> getSources() {
    return sources;
  }

  public void setSources(Map<String, DataSource> sources) {
    this.sources = sources;
  }

  public Collection<FeatureLayer> getLayers() {
    return layers;
  }

  public void setLayers(Collection<FeatureLayer> layers) {
    this.layers = layers;
  }

  public Map<String, Object> getInputMappings() {
    return inputMappings;
  }

  public void setInputMappings(Map<String, Object> inputMappings) {
    this.inputMappings = inputMappings;
  }
}
