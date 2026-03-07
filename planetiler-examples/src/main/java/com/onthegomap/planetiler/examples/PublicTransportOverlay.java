package com.onthegomap.planetiler.examples;

import com.onthegomap.planetiler.FeatureCollector;
import com.onthegomap.planetiler.Planetiler;
import com.onthegomap.planetiler.Profile;
import com.onthegomap.planetiler.config.Arguments;
import com.onthegomap.planetiler.reader.SourceFeature;
import java.nio.file.Path;

public class PublicTransportOverlay implements Profile {

  // For now, create an overlay that displays tram lines and their stops
  @Override
  public void processFeature(SourceFeature sourceFeature, FeatureCollector features) {
    if (sourceFeature.isPoint() && sourceFeature.hasTag("railway", "tram_stop")) {
      features.point("Tram stop")
        .setAttr("name", sourceFeature.getTag("name"));
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
    run(Arguments.fromArgsOrConfigFile(args));
  }

  static void run(Arguments args) throws Exception {
    String area = args.getString("area", "geofabrik area to download", "Berlin");
    Planetiler.create(args)
      .setProfile(new PublicTransportOverlay())
      // if input.pbf not found, download Berlin from Geofabrik
      .addOsmSource("osm", Path.of("data", "sources", area + ".osm.pbf"), "geofabrik:" + area)
      .overwriteOutput(Path.of("data", "publictransport.mbtiles"))
      .run();
  }
}
