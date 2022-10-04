package com.onthegomap.planetiler.custommap;

import com.onthegomap.planetiler.custommap.configschema.DataSourceType;
import java.nio.file.Path;

/** A parsed source definition from a config file. */
public record Source(
  String id,
  DataSourceType type,
  String url,
  Path localPath
) {

  public String defaultFileUrl() {
    String result = url
      .replaceFirst("^https?://", "")
      .replaceAll("[\\W&&[^.]]+", "_");
    if (type == DataSourceType.OSM && !result.endsWith(".pbf")) {
      result = result + ".osm.pbf";
    }
    return result;
  }
}
