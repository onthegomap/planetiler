package com.onthegomap.planetiler.examples;

import com.onthegomap.planetiler.FeatureCollector;
import com.onthegomap.planetiler.Profile;
import com.onthegomap.planetiler.reader.SourceFeature;

public class PublicTransportOverlay implements Profile {

  @Override
  public void processFeature(SourceFeature sourceFeature, FeatureCollector features) {
    //
  }

  @Override
  public String name() {
    // name that shows up in the MBTiles metadata table
    return "Public Transport Overlay";
  }

  public static void main(String[] args) throws Exception {
    /*
    Planetiler.create(args)
      .setProfile(new MyProfile())
      // if input.pbf not found, download Berlin from Geofabrik
      .addOsmSource("osm", Path.of("data", "sources", "input.pbf"), "geofabrik:Berlin")
      .overwriteOutput("mbtiles", Path.of("data", "toilets.mbtiles"))
      .run();
     */
  }
}
