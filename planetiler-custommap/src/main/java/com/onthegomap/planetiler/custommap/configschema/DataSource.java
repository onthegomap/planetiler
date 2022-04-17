package com.onthegomap.planetiler.custommap.configschema;

public class DataSource {
  private String name;
  private DataSourceType type;
  private String url;

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public DataSourceType getType() {
    return type;
  }

  public void setType(DataSourceType type) {
    this.type = type;
  }

  public String getUrl() {
    return url;
  }

  public void setUrl(String url) {
    this.url = url;
  }
}
