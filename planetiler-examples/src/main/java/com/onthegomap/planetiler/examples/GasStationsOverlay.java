package com.onthegomap.planetiler.examples;

import com.onthegomap.planetiler.FeatureCollector;
import com.onthegomap.planetiler.Planetiler;
import com.onthegomap.planetiler.Profile;
import com.onthegomap.planetiler.config.Arguments;
import com.onthegomap.planetiler.reader.SourceFeature;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.time.DayOfWeek;
import java.time.format.DateTimeFormatter;
import java.util.regex.Pattern;

/**
 * this example profile shows gasstations on the map and hours theyre open at.
 * href="https://wiki.openstreetmap.org/wiki/Tag:amenity%3Dfuel"
 */

public class GasStationOverlay implements Profile {

  @Override
  public void processFeature(SourceFeature sourceFeature, FeatureCollector features) {
    // Check if this feature is a gas station (amenity=fuel)
    boolean isGasStation = sourceFeature.hasTag("amenity", "fuel");
    
    if (isGasStation) {
      // Get the brand name - use as name if name tag is missing
      String brand =(String) sourceFeature.getTag("brand");
      String name = (String) sourceFeature.getTag("name");
      
      // If name is missing but brand exists, use brand as the name
      String displayName = (name != null && !name.isEmpty()) ? name : brand;
      
      // Get opening hours and determine if currently open
      String openingHours =(String) sourceFeature.getTag("opening_hours");
      String openStatus = checkIfOpen(openingHours);
      
      // Handle nodes (point at center of gas station)
      if (sourceFeature.isPoint()) {
        features.point("gas_station")
          .setAttr("name", displayName)
          .setAttr("brand", brand)
          .setAttr("operator", sourceFeature.getTag("operator"))
          .setAttr("hgv", sourceFeature.getTag("hgv"))  // Large vehicle access
          .setAttr("opening_hours", openingHours)
          .setAttr("open_status", openStatus)  // "open", "closed", or "unknown"
          .setMinZoom(12);
      } 
      // Handle areas (polygon around fueling area)
      else if (sourceFeature.canBePolygon()) {
        features.polygon("gas_station")
          .setAttr("name", displayName)
          .setAttr("brand", brand)
          .setAttr("operator", sourceFeature.getTag("operator"))
          .setAttr("hgv", sourceFeature.getTag("hgv"))
          .setAttr("opening_hours", openingHours)
          .setAttr("open_status", openStatus)
          .setMinZoom(12);
      }
    }
  }
  
  /**
   * Simple check if a gas station is currently open based on opening_hours tag.
   */
  private String checkIfOpen(String openingHours) {
    if (openingHours == null || openingHours.isEmpty()) {
      return "unknown";
    }
    
    // Check for 24/7
    if (openingHours.equals("24/7") || openingHours.equals("24-7")) {
      return "open";
    }
    
    // Get current time
    LocalDateTime now = LocalDateTime.now();
    DayOfWeek today = now.getDayOfWeek();
    int currentHour = now.getHour();
    int currentMinute = now.getMinute();
    double currentTime = currentHour + (currentMinute / 60.0);
    
    // Simple pattern matching for common formats
    String hoursLower = openingHours.toLowerCase();
    
    // Check if closed on this day
    String[] days = {"mo", "tu", "we", "th", "fr", "sa", "su"};
    String todayAbbr = days[today.getValue() - 1]; // Monday=1 -> index 0
    
    if (hoursLower.contains(todayAbbr + " off") || hoursLower.contains(todayAbbr + " closed")) {
      return "closed";
    }
    
    // Check for simple time ranges like "Mo-Fr 08:00-20:00"
    Pattern timePattern = Pattern.compile(todayAbbr + "[^0-9]*([0-9]{1,2}):?([0-9]{2})?-([0-9]{1,2}):?([0-9]{2})?");
    var matcher = timePattern.matcher(hoursLower);
    
    if (matcher.find()) {
      int openHour = Integer.parseInt(matcher.group(1));
      int closeHour = Integer.parseInt(matcher.group(3));
      double openTime = openHour;
      double closeTime = closeHour;
      
      if (currentTime >= openTime && currentTime < closeTime) {
        return "open";
      } else {
        return "closed";
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
    String area = args.getString("area", "geofabrik area to download", "monaco");
    
    Planetiler.create(args)
      .setProfile(new GasStationOverlay())
      .addOsmSource("osm", Path.of("data", "sources", area + ".osm.pbf"), "geofabrik:" + area)
      .overwriteOutput(Path.of("data", "gasstations.mbtiles"))
      .run();
  }
}