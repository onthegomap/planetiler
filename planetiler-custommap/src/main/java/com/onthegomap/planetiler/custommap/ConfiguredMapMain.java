package com.onthegomap.planetiler.custommap;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.onthegomap.planetiler.Planetiler;
import com.onthegomap.planetiler.config.Arguments;
import com.onthegomap.planetiler.custommap.configschema.DataSource;
import com.onthegomap.planetiler.custommap.configschema.DataSourceType;
import com.onthegomap.planetiler.custommap.configschema.SchemaConfig;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import org.yaml.snakeyaml.Yaml;

/**
 * Main driver to create maps configured by a YAML file.
 *
 * Parses the config file into a {@link ConfiguredProfile}, loads sources into {@link Planetiler} runner and kicks off
 * the map generation process.
 */
public class ConfiguredMapMain {

  private static final Yaml yaml = new Yaml();
  private static final ObjectMapper mapper = new ObjectMapper();

  /*
   * Main entrypoint
   */
  public static void main(String... args) throws Exception {
    run(Arguments.fromArgsOrConfigFile(args));
  }

  static void run(Arguments args) throws Exception {
    var dataDir = Path.of("data");
    var sourcesDir = dataDir.resolve("sources");

    var schemaFile = args.inputFile(
      "schema",
      "Location of YML-format schema definition file");

    var config = loadConfig(schemaFile);

    var planetiler = Planetiler.create(args)
      .setProfile(new ConfiguredProfile(config));

    var sources = config.sources();
    for (var source : sources.entrySet()) {
      configureSource(planetiler, sourcesDir, source.getKey(), source.getValue());
    }

    planetiler.overwriteOutput("mbtiles", Path.of("data", "output.mbtiles"))
      .run();
  }

  static SchemaConfig loadConfig(Path schemaFile) throws IOException {
    try (var schemaStream = Files.newInputStream(schemaFile)) {
      Map<String, Object> parsed = yaml.load(schemaStream);
      return mapper.convertValue(parsed, SchemaConfig.class);
    }
  }

  private static void configureSource(Planetiler planetiler, Path sourcesDir, String sourceName, DataSource source)
    throws URISyntaxException {

    DataSourceType sourceType = source.type();
    Path localPath = source.localPath();

    switch (sourceType) {
      case OSM -> {
        String url = source.url();
        String[] areaParts = url.split("[:/]");
        String areaFilename = areaParts[areaParts.length - 1];
        String areaName = areaFilename.replaceAll("\\..*$", "");
        if (localPath == null) {
          localPath = sourcesDir.resolve(areaName + ".osm.pbf");
        }
        planetiler.addOsmSource(sourceName, localPath, url);
      }
      case SHAPEFILE -> {
        String url = source.url();
        if (localPath == null) {
          localPath = sourcesDir.resolve(Paths.get(new URI(url).getPath()).getFileName().toString());
        }
        planetiler.addShapefileSource(sourceName, localPath, url);
      }
      default -> throw new IllegalArgumentException("Unhandled source " + sourceType);
    }
  }
}
