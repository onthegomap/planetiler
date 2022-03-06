package com.onthegomap.planetiler.config;

import com.onthegomap.planetiler.Profile;
import com.onthegomap.planetiler.mbtiles.MbtilesWriter;

/** Controls information that {@link MbtilesWriter} will write to the mbtiles metadata table. */
public record MbtilesMetadata(
    String name, String description, String attribution, String version, String type) {

  public MbtilesMetadata(Profile profile) {
    this(
        profile.name(),
        profile.description(),
        profile.attribution(),
        profile.version(),
        profile.isOverlay() ? "overlay" : "baselayer");
  }

  public MbtilesMetadata(Profile profile, Arguments args) {
    this(
        args.getString("mbtiles_name", "'name' attribute for mbtiles metadata", profile.name()),
        args.getString(
            "mbtiles_description",
            "'description' attribute for mbtiles metadata",
            profile.description()),
        args.getString(
            "mbtiles_attribution",
            "'attribution' attribute for mbtiles metadata",
            profile.attribution()),
        args.getString(
            "mbtiles_version", "'version' attribute for mbtiles metadata", profile.version()),
        args.getString(
            "mbtiles_type",
            "'type' attribute for mbtiles metadata",
            profile.isOverlay() ? "overlay" : "baselayer"));
  }
}
