package com.onthegomap.flatmap.openmaptiles;

import com.onthegomap.flatmap.FlatmapRunner;
import com.onthegomap.flatmap.config.Arguments;
import com.onthegomap.flatmap.openmaptiles.generated.OpenMapTilesSchema;
import java.nio.file.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OpenMapTilesMain {

  private static final Logger LOGGER = LoggerFactory.getLogger(OpenMapTilesMain.class);
  private static final Path sourcesDir = Path.of("data", "sources");

  public static void main(String[] args) throws Exception {
    run(Arguments.fromArgsOrConfigFile(args));
  }

  static void run(Arguments arguments) throws Exception {
    FlatmapRunner.create(arguments)
      .setDefaultLanguages(OpenMapTilesSchema.LANGUAGES)
      .fetchWikidataNameTranslations(sourcesDir.resolve("wikidata_names.json"))
      .setProfile(OpenMapTilesProfile::new)
      .addShapefileSource("EPSG:3857", OpenMapTilesProfile.LAKE_CENTERLINE_SOURCE,
        sourcesDir.resolve("lake_centerline.shp.zip"),
        "https://github.com/lukasmartinelli/osm-lakelines/releases/download/v0.9/lake_centerline.shp.zip")
      .addShapefileSource(OpenMapTilesProfile.WATER_POLYGON_SOURCE,
        sourcesDir.resolve("water-polygons-split-3857.zip"),
        "https://osmdata.openstreetmap.de/download/water-polygons-split-3857.zip")
      .addNaturalEarthSource(OpenMapTilesProfile.NATURAL_EARTH_SOURCE,
        sourcesDir.resolve("natural_earth_vector.sqlite.zip"),
        "https://naturalearth.s3.amazonaws.com/packages/natural_earth_vector.sqlite.zip")
      // TODO: "https://naciscdn.org/naturalearth/packages/natural_earth_vector.sqlite.zip")
      .addOsmSource(OpenMapTilesProfile.OSM_SOURCE, sourcesDir.resolve("input.osm.pbf"), "geofabrik:monaco")
      .setOutput("mbtiles", Path.of("data", "output.mbtiles"))
      .run();
  }
}
