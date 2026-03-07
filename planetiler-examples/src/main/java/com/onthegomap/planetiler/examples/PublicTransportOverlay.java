package com.onthegomap.planetiler.examples;

import com.onthegomap.planetiler.FeatureCollector;
import com.onthegomap.planetiler.Profile;
import com.onthegomap.planetiler.reader.SourceFeature;

public class PublicTransportOverlay implements Profile {

  @Override
  public void processFeature(SourceFeature sourceFeature, FeatureCollector features) {
    if (sourceFeature.canBeLine()) {
      //
    }
  }

  @Override
  public boolean isOverlay() {
    return true;
  }

  @Override
  public String name() {
    // name that shows up in the MBTiles metadata table
    return "Public Transport Overlay";
  }

  @Override
  public String description() {
    return "An example overlay that shows tram routes and stops";
  }

  /*
   * Any time you use OpenStreetMap data, you must ensure clients display the following copyright. Most clients will
   * display this automatically if you populate it in the attribution metadata in the mbtiles file:
   */
  @Override
  public String attribution() {
    return """
      <a href="https://www.openstreetmap.org/copyright" target="_blank">&copy; OpenStreetMap contributors</a>
      """.trim();
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
