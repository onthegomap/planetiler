package com.onthegomap.planetiler.custommap.configschema;

import java.util.Collection;

public class SchemaConfig {
  private String schemaName;
  private String schemaDescription;
  private String attribution =
    "<a href=\\\"https://www.openstreetmap.org/copyright\\\" target=\\\"_blank\\\">&copy; OpenStreetMap contributors</a>";
  private Collection<DataSource> sources;
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

  public Collection<DataSource> getSources() {
    return sources;
  }

  public void setSources(Collection<DataSource> sources) {
    this.sources = sources;
  }

  public Collection<FeatureLayer> getLayers() {
    return layers;
  }

  public void setLayers(Collection<FeatureLayer> layers) {
    this.layers = layers;
  }
}
