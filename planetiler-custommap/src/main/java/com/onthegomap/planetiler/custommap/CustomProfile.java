package com.onthegomap.planetiler.custommap;

import java.io.File;
import java.io.FileInputStream;
import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;

import org.yaml.snakeyaml.Yaml;

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
        Paths.get("src", "main", "resources", "schemas", "owg_simple.yml").toString());

    Yaml yml = new Yaml();
    Map<String, Object> config = (Map<String, Object>) yml.load(new FileInputStream(new File(schemaFile)));
    System.out.println(config);

    // Planetiler is a convenience wrapper around the lower-level API for the most common use-cases.
    Planetiler planetiler = Planetiler.create(args)
      .setProfile(new ConfiguredProfile(config));

    List<Object> sources = (List<Object>) config.get("sources");
    for (int i = 0; i < sources.size(); i++) {
      configureSource(planetiler, sourcesDir, (Map<String, Object>) sources.get(i));
    }

    planetiler.overwriteOutput("mbtiles", Path.of("data", "spartan.mbtiles"))
      .run();
  }

  private static void configureSource(Planetiler planetiler, Path sourcesDir, Map<String, Object> sources)
    throws Exception {

    String sourceType = YamlParser.getString(sources, "type");
    String sourceName = YamlParser.getString(sources, "name");

    switch (sourceType) {
      case "osm":
        String area = YamlParser.getString(sources, "area");
        String[] areaParts = area.split(":");
        String areaName = areaParts[areaParts.length - 1];
        planetiler.addOsmSource(sourceName, sourcesDir.resolve(areaName + ".osm.pbf"), area);
        return;
      case "shapefile":
        String url = YamlParser.getString(sources, "url");
        String filename = Paths.get(new URI(url).getPath()).getFileName().toString();
        planetiler.addShapefileSource(sourceName, sourcesDir.resolve(filename), url);
        return;
      default:
        throw new IllegalArgumentException("Uhandled source " + sourceType);
    }
  }
}
