package com.onthegomap.planetiler.custommap.configschema;

public class DataSource {
  private DataSourceType type;
  private String url;

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
