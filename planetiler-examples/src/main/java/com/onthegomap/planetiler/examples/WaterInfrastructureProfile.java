package com.onthegomap.planetiler.examples;

import com.onthegomap.planetiler.FeatureCollector;
import com.onthegomap.planetiler.Planetiler;
import com.onthegomap.planetiler.Profile;
import com.onthegomap.planetiler.config.Arguments;
import com.onthegomap.planetiler.reader.SourceFeature;
import java.nio.file.Path;


/**
 * This profile extracts nodes from a .osd.pbf file (map) and exclusively makes up vector tiles of region with water
 * infrastructures, with an intention to see water resources across that region, with current location of interest being
 * monaco.
 * <p>
 * See <a href="https://wiki.openstreetmap.org/wiki/Key:admin_level">OpenStreetMap admin_level documentation</a> for
 * details on administrative boundary levels.
 * <p>
 * It is recommended to try this example using monaco data as a source with various water infrastructures and small in
 * size for low end pc's.
 * <p>
 * To run this example:
 * <ol>
 * <li>Download a .osm.pbf extract (see <a href="https://download.geofabrik.de/">Geofabrik download site</a>) (Central
 * monaco is recommended)</li>
 * <li>Move the file into {@code planetiler-examples/data/sources/}</li>
 * <li>Build the examples: {@code mvn clean package --file standalone.pom.xml }</li>
 * <li>Run this example:
 * {@code java -cp target/*-with-deps.jar com.onthegomap.planetiler.examples.WaterInfrastructureProfile --osm-path=./data/sources/central-america.osm.pbf"}</li>
 * <li>Run the demo tileserver: {@code tileserver-gl-light data/admin-borders.mbtiles}</li>
 * <li>View the output at <a href="http://localhost:8080">localhost:8080</a></li>
 * </ol>
 */


public class WaterInfrastructureProfile implements Profile {
  public static void main(String[] args) {
    //1.Tell Planetiler to use your Profile
    var profile = new WaterInfrastructureProfile();
    //2.Start the engine with empty arguments
    Planetiler.create(Arguments.fromArgs(args))
      .setProfile(profile)
      .addOsmSource("osm", Path.of("monaco.osm.pbf"), "georfabrik:monaco")
      .setOutput(Path.of("water_map.mbtiles"))
      .run();

  }

  public void processFeature(SourceFeature source, FeatureCollector features) {

    //1. Primary water sources like wells, taps, pumps...
    if (source.hasTag("man_made", "water_well", "water_tap", "water_tap", "water_well") ||
      source.hasTag("amenity", "drinking_water", "water_point")) {

      features.point("water_supply")
        .setAttr("name", source.getString("name"))
        .setAttr("source", source.getTag("man_made"))// is it a well or a pump
        .setAttr("operator", source.getString("operator")) // who maintains it ?
        .setZoomRange(10, 14);
    }
    //2. Water storages ( Reservoirs or tanks)
    if (source.hasTag("man_made", "storage_tank", "water_tower") ||
      source.hasTag("landuse", "reservoir")) {

      features.point("water_storage")
        .setAttr("type", source.getTag("man_made"))
        .setZoomRange(11, 14);
    }
  }

  @Override
  public String name() {
    return "Water Infrastructure Map";
  }

  @Override
  public String description() {
    return "Mapping water and amenities for GSoC project.";
  }

  @Override
  public String attribution() {
    return "© OpenStreetMap contributors";
  }

}
