package com.onthegomap.planetiler.archive;

import com.onthegomap.planetiler.Profile;
import com.onthegomap.planetiler.config.Arguments;
import com.onthegomap.planetiler.util.BuildInfo;
import java.util.LinkedHashMap;
import java.util.Map;

/** Controls information that {@link TileArchiveWriter} will write to the archive metadata. */
public record TileArchiveMetadata(
  String name,
  String description,
  String attribution,
  String version,
  String type,
  Map<String, String> planetilerSpecific
) {

  public TileArchiveMetadata(Profile profile) {
    this(
      profile.name(),
      profile.description(),
      profile.attribution(),
      profile.version(),
      profile.isOverlay() ? "overlay" : "baselayer",
      mapWithBuildInfo()
    );
  }

  public TileArchiveMetadata(Profile profile, Arguments args) {
    this(
      args.getString("mbtiles_name", "'name' attribute for tileset metadata", profile.name()),
      args.getString("mbtiles_description", "'description' attribute for tileset metadata", profile.description()),
      args.getString("mbtiles_attribution", "'attribution' attribute for tileset metadata", profile.attribution()),
      args.getString("mbtiles_version", "'version' attribute for tileset metadata", profile.version()),
      args.getString("mbtiles_type", "'type' attribute for tileset metadata",
        profile.isOverlay() ? "overlay" : "baselayer"),
      mapWithBuildInfo()
    );
  }

  private static Map<String, String> mapWithBuildInfo() {
    Map<String, String> result = new LinkedHashMap<>();
    var buildInfo = BuildInfo.get();
    if (buildInfo != null) {
      if (buildInfo.version() != null) {
        result.put("planetiler:version", buildInfo.version());
      }
      if (buildInfo.githash() != null) {
        result.put("planetiler:githash", buildInfo.githash());
      }
      if (buildInfo.buildTimeString() != null) {
        result.put("planetiler:buildtime", buildInfo.buildTimeString());
      }
    }
    return result;
  }

  public TileArchiveMetadata set(String key, Object value) {
    if (key != null && value != null) {
      planetilerSpecific.put(key, value.toString());
    }
    return this;
  }

  public Map<String, String> getAll() {
    var allKvs = new LinkedHashMap<String, String>(planetilerSpecific);
    allKvs.put("name", this.name);
    allKvs.put("description", this.description);
    allKvs.put("attribution", this.attribution);
    allKvs.put("version", this.version);
    allKvs.put("type", this.type);
    return allKvs;
  }
}
