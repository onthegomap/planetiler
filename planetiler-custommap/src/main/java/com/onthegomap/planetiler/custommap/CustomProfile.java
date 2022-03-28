package com.onthegomap.planetiler.custommap;

import java.io.File;
import java.net.URI;
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

    //TODO move to command-line args
    String schemaFile =
      args.getString("schema", "mbtiles schema",
        Paths.get("src", "main", "resources", "schemas", "owg_simple.json").toString());

    ObjectMapper mapper = new ObjectMapper();
    JsonNode schemaRoot = mapper.readTree(new File(schemaFile));

    // Planetiler is a convenience wrapper around the lower-level API for the most common use-cases.
    Planetiler planetiler = Planetiler.create(args)
      .setProfile(new ConfiguredProfile(schemaRoot));

    JsonNode sourcesJson = schemaRoot.get("sources");
    for (int i = 0; i < sourcesJson.size(); i++) {
      configureSource(planetiler, sourcesDir, sourcesJson.get(i));
    }

    planetiler.overwriteOutput("mbtiles", Path.of("data", "spartan.mbtiles"))
      .run();
  }

  private static void configureSource(Planetiler planetiler, Path sourcesDir, JsonNode sourcesJson) throws Exception {
    String sourceType = sourcesJson.get("type").asText();
    String sourceName = sourcesJson.get("name").asText();

    switch (sourceType) {
      case "osm":
        String area = sourcesJson.get("area").asText();
        String[] areaParts = area.split(":");
        String areaName = areaParts[areaParts.length - 1];
        planetiler.addOsmSource(sourceName, sourcesDir.resolve(areaName + ".osm.pbf"), area);
        return;
      case "shapefile":
        String url = sourcesJson.get("url").asText();
        String filename = Paths.get(new URI(url).getPath()).getFileName().toString();
        planetiler.addShapefileSource(sourceName, sourcesDir.resolve(filename), url);
        return;
      default:
        throw new IllegalArgumentException("Uhandled source " + sourceType);
    }
  }
}
