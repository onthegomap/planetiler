package com.onthegomap.planetiler.config;

import com.onthegomap.planetiler.Profile;
import com.onthegomap.planetiler.mbtiles.MbtilesWriter;
import com.onthegomap.planetiler.util.BuildInfo;
import java.util.LinkedHashMap;
import java.util.Map;

/** Controls information that {@link MbtilesWriter} will write to the mbtiles metadata table. */
public record MbtilesMetadata(
  String name,
  String description,
  String attribution,
  String version,
  String type,
  Map<String, String> planetilerSpecific
) {

  public MbtilesMetadata(Profile profile) {
    this(
      profile.name(),
      profile.description(),
      profile.attribution(),
      profile.version(),
      profile.isOverlay() ? "overlay" : "baselayer",
      mapWithBuildInfo()
    );
  }

  public MbtilesMetadata(Profile profile, Arguments args) {
    this(
      args.getString("mbtiles_name", "'name' attribute for mbtiles metadata", profile.name()),
      args.getString("mbtiles_description", "'description' attribute for mbtiles metadata", profile.description()),
      args.getString("mbtiles_attribution", "'attribution' attribute for mbtiles metadata", profile.attribution()),
      args.getString("mbtiles_version", "'version' attribute for mbtiles metadata", profile.version()),
      args.getString("mbtiles_type", "'type' attribute for mbtiles metadata",
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

  public MbtilesMetadata set(String key, Object value) {
    if (key != null && value != null) {
      planetilerSpecific.put(key, value.toString());
    }
    return this;
  }
}
