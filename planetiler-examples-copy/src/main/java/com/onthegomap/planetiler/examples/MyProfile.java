package com.onthegomap.planetiler.examples;

import com.onthegomap.planetiler.FeatureCollector;
import com.onthegomap.planetiler.Planetiler;
import com.onthegomap.planetiler.Profile;
import com.onthegomap.planetiler.config.Arguments;
import com.onthegomap.planetiler.geo.GeometryException;
import com.onthegomap.planetiler.reader.SourceFeature;
import java.nio.file.Path;

public class MyProfile implements Profile {

  @Override
  public String name() {
    // name that shows up in the MBTiles metadata table
    return "My Profile";
  }


//  @Override
//  public void processFeature(SourceFeature sourceFeature, FeatureCollector features) {
//    if (sourceFeature.isPoint() && sourceFeature.hasTag("amenity", "toilets")) {
//      features.point("toilets") // create a point in layer named "toilets"
//        .setMinZoom(12)
//        .setAttr("customers_only", sourceFeature.hasTag("access", "customers"))
//        .setAttr("indoor", sourceFeature.getBoolean("indoor"))
//        .setAttr("name", sourceFeature.getTag("name"))
//        .setAttr("operator", sourceFeature.getTag("operator"));
//    }
//  }
//  @Override
//  public void processFeature(SourceFeature sourceFeature, FeatureCollector features) {
//    if (sourceFeature.isPoint()) {
//      features.point("points")
//        .setMinZoom(5)
//        .setAttr("type", sourceFeature.getTag("amenity"));
//    }
//  }
  @Override
  public void processFeature(SourceFeature sourceFeature, FeatureCollector features) {
    // Cafes
    if (sourceFeature.isPoint() && sourceFeature.hasTag("amenity", "cafe")) {
      features.point("cafes")
        .setMinZoom(12)
        .setAttr("name", sourceFeature.getTag("name"));
    }
    // Roads
    if (sourceFeature.canBeLine() && sourceFeature.hasTag("highway")) {
      features.line("roads")
        .setMinZoom(8)
        .setAttr("type", sourceFeature.getTag("highway"));
    }
  }

  public static void main(String... args) throws Exception {
    Planetiler.create(Arguments.fromArgs(args))
      .setProfile(new MyProfile())
      // if input.pbf not found, download Monaco from Geofabrik
      .addOsmSource("osm", Path.of("data", "sources", "input.pbf"), "geofabrik:monaco")
      .setOutput(Path.of("data", "toilets.mbtiles"))
      .run();
  }
}
