package com.onthegomap.planetiler.custommap;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.onthegomap.planetiler.Planetiler;
import com.onthegomap.planetiler.config.Arguments;
import com.onthegomap.planetiler.custommap.configschema.DataSource;
import com.onthegomap.planetiler.custommap.configschema.DataSourceType;
import com.onthegomap.planetiler.custommap.configschema.SchemaConfig;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import org.yaml.snakeyaml.Yaml;

public class ConfiguredMapMain {

  private static final Yaml yaml = new Yaml();
  private static final ObjectMapper mapper = new ObjectMapper();

  /*
   * Main entrypoint
   */
  public static void main(String[] args) throws Exception {
    run(Arguments.fromArgsOrConfigFile(args));
  }

  private static void run(Arguments args) throws Exception {
    var dataDir = Path.of("data");
    var sourcesDir = dataDir.resolve("sources");

    var schemaFile = args.inputFile(
      "schema",
      "Location of YML-format schema definition file");

    var config = loadConfig(schemaFile);

    var planetiler = Planetiler.create(args)
      .setProfile(new ConfiguredProfile(config));

    var sources = config.getSources();
    for (var source : sources.entrySet()) {
      configureSource(planetiler, sourcesDir, source.getKey(), source.getValue());
    }

    planetiler.overwriteOutput("mbtiles", Path.of("data", "output.mbtiles"))
      .run();
  }

  static SchemaConfig loadConfig(Path schemaFile) throws IOException {
    Map<String, Object> parsed = yaml.load(Files.newInputStream(schemaFile));
    return mapper.convertValue(parsed, SchemaConfig.class);
  }

  private static void configureSource(Planetiler planetiler, Path sourcesDir, String sourceName, DataSource source)
    throws Exception {

    DataSourceType sourceType = source.type();

    switch (sourceType) {
      case osm:
        String area = source.url();
        String[] areaParts = area.split("[:/]");
        String areaFilename = areaParts[areaParts.length - 1];
        String areaName = areaFilename.replaceAll("\\..*?$", "");
        planetiler.addOsmSource(sourceName, sourcesDir.resolve(areaName + ".osm.pbf"), area);
        return;
      case shapefile:
        String url = source.url();
        String filename = Paths.get(new URI(url).getPath()).getFileName().toString();
        planetiler.addShapefileSource(sourceName, sourcesDir.resolve(filename), url);
        return;
      default:
        throw new IllegalArgumentException("Uhandled source " + sourceType);
    }
  }
}
