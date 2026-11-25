package com.onthegomap.planetiler.examples;

import com.onthegomap.planetiler.FeatureCollector;
import com.onthegomap.planetiler.Planetiler;
import com.onthegomap.planetiler.Profile;
import com.onthegomap.planetiler.config.Arguments;
import com.onthegomap.planetiler.reader.SourceFeature;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.time.DayOfWeek;
import java.util.regex.Pattern;

/**
 * this example profile shows gasstations on the map and hours theyre open at.
 * href="https://wiki.openstreetmap.org/wiki/Tag:amenity%3Dfuel"
 */

public class GasStationsOverlay implements Profile {

  @Override
  public void processFeature(SourceFeature sourceFeature, FeatureCollector features) {
    // Check if this feature is a gas station (amenity=fuel)
    boolean isGasStation = sourceFeature.hasTag("amenity", "fuel");
    
    if (isGasStation) {
      // Get the brand name - use as name if name tag is missing
      String brand = sourceFeature.getString("brand");
      String name = sourceFeature.getString("name");
      
      // If name is missing but brand exists, use brand as the name
      String displayName = (name != null && !name.isEmpty()) ? name : brand;
      
      // Get opening hours and determine if currently open
      String openingHours = sourceFeature.getString("opening_hours");
      
     
        features.anyGeometry("gas_station")
          .setAttr("name", displayName)
          .setAttr("brand", brand)
          .setAttr("operator", sourceFeature.getTag("operator"))
          .setAttr("hgv", sourceFeature.getTag("hgv"))  // Large vehicle access
          .setAttr("opening_hours", openingHours)
          .setMinZoom(12); 
     
    }
  }
  
    // Check for "sunrise-sunset" (always treat as open - would need external data for real calculation)
    if (hoursLower.contains("sunrise") || hoursLower.contains("sunset")) {
      return "unknown";
    }
    
    // Default for complex formats not parsed
    return "unknown";
  }

  @Override
  public boolean isOverlay() {
    return true;
  }

  @Override
  public String name() {
    return "Gas Stations Overlay";
  }

  @Override
  public String description() {
    return "Shows gas stations including brand, operator, HGV access, and opening status";
  }

  public static void main(String[] args) {
    run(Arguments.fromArgsOrConfigFile(args));
  }

  static void run(Arguments args) {
    String area = args.getString("area", "geofabrik area to download", "luxembourg");
    
    Planetiler.create(args)
      .setProfile(new GasStationsOverlay())
      .addOsmSource("osm", Path.of("data", "sources", area + ".osm.pbf"), "geofabrik:" + area)
      .overwriteOutput(Path.of("data", "gasstations.mbtiles"))
      .run();
  }
}
