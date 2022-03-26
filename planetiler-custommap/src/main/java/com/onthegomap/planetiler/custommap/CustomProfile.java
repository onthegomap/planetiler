package com.onthegomap.planetiler.custommap;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.onthegomap.planetiler.Planetiler;
import com.onthegomap.planetiler.config.Arguments;

public class CustomProfile {

  /*
   * Main entrypoint for the example program
   */
  public static void main(String[] args) throws Exception {
    run(Arguments.fromArgsOrConfigFile(args));
  }

  static void run(Arguments args) throws Exception {
    Path dataDir = Path.of("data");
    Path sourcesDir = dataDir.resolve("sources");

    String area = args.getString("area", "geofabrik area to download", "rhode-island");
    String schemaFile =
      args.getString("schema", "mbtiles schema",
        Paths.get("src", "main", "resources", "schemas", "owg_simple.json").toString());

    ObjectMapper mapper = new ObjectMapper();
    JsonNode schemaRoot = mapper.readTree(new File(schemaFile));

    // Planetiler is a convenience wrapper around the lower-level API for the most common use-cases.
    Planetiler.create(args)
      .setProfile(new ConfiguredProfile(schemaRoot))

      //TODO -- configure
      .addShapefileSource("water_polygons",
        sourcesDir.resolve("water-polygons-split-3857.zip"),
        "https://osmdata.openstreetmap.de/download/water-polygons-split-3857.zip") // override this default with osm_path="path/to/data.osm.pbf"

      //TODO -- configure
      .addOsmSource("osm", Path.of("data", "sources", area + ".osm.pbf"), "geofabrik:" + area)

      // override this default with mbtiles="path/to/output.mbtiles"
      .overwriteOutput("mbtiles", Path.of("data", "spartan.mbtiles"))
      .run();
  }
}
