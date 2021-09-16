package com.onthegomap.flatmap.openmaptiles;

import com.onthegomap.flatmap.FlatmapRunner;
import com.onthegomap.flatmap.config.Arguments;
import com.onthegomap.flatmap.openmaptiles.generated.OpenMapTilesSchema;
import java.nio.file.Path;

/**
 * Main entrypoint for generating a map using the OpenMapTiles schema.
 */
public class OpenMapTilesMain {

  public static void main(String[] args) throws Exception {
    run(Arguments.fromArgsOrConfigFile(args));
  }

  static void run(Arguments arguments) throws Exception {
    Path dataDir = Path.of("data");
    Path sourcesDir = dataDir.resolve("sources");
    // use --area=... argument, AREA=... env var or area=... in config to set the region of the world to use
    // will be ignored if osm_path or osm_url are set
    String area = arguments.getString(
      "area",
      "name of the geofabrik extract to download if osm_url/osm_path not specified (i.e. 'monaco' 'rhode island' or 'australia')",
      "monaco"
    );

    FlatmapRunner.create(arguments)
      .setDefaultLanguages(OpenMapTilesSchema.LANGUAGES)
      .fetchWikidataNameTranslations(sourcesDir.resolve("wikidata_names.json"))
      // defer creation of the profile because it depends on data from the runner
      .setProfile(OpenMapTilesProfile::new)
      // override any of these with arguments: --osm_path=... or --osm_url=...
      // or OSM_PATH=... OSM_URL=... environmental argument
      // or osm_path=... osm_url=... in a config file
      .addShapefileSource("EPSG:3857", OpenMapTilesProfile.LAKE_CENTERLINE_SOURCE,
        sourcesDir.resolve("lake_centerline.shp.zip"),
        "https://github.com/lukasmartinelli/osm-lakelines/releases/download/v0.9/lake_centerline.shp.zip")
      .addShapefileSource(OpenMapTilesProfile.WATER_POLYGON_SOURCE,
        sourcesDir.resolve("water-polygons-split-3857.zip"),
        "https://osmdata.openstreetmap.de/download/water-polygons-split-3857.zip")
      .addNaturalEarthSource(OpenMapTilesProfile.NATURAL_EARTH_SOURCE,
        sourcesDir.resolve("natural_earth_vector.sqlite.zip"),
        "https://naturalearth.s3.amazonaws.com/packages/natural_earth_vector.sqlite.zip") // TODO: go back to "https://naciscdn.org/naturalearth/packages/natural_earth_vector.sqlite.zip")
      .addOsmSource(OpenMapTilesProfile.OSM_SOURCE,
        sourcesDir.resolve(area.replaceAll("[^a-zA-Z]+", "_") + ".osm.pbf"),
        "geofabrik:" + area)
      // override with --mbtiles=... argument or MBTILES=... env var or mbtiles=... in a config file
      .setOutput("mbtiles", dataDir.resolve("output.mbtiles"))
      .run();
  }
}
