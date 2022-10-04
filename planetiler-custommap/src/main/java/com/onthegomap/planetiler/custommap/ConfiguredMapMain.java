package com.onthegomap.planetiler.custommap;

import com.onthegomap.planetiler.Planetiler;
import com.onthegomap.planetiler.config.Arguments;
import com.onthegomap.planetiler.custommap.configschema.DataSourceType;
import com.onthegomap.planetiler.custommap.configschema.SchemaConfig;
import com.onthegomap.planetiler.custommap.expression.ParseException;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Main driver to create maps configured by a YAML file.
 *
 * Parses the config file into a {@link ConfiguredProfile}, loads sources into {@link Planetiler} runner and kicks off
 * the map generation process.
 */
public class ConfiguredMapMain {

  /*
   * Main entrypoint
   */
  public static void main(String... args) throws Exception {
    Arguments arguments = Arguments.fromEnvOrArgs(args);
    var dataDir = Path.of("data");
    var sourcesDir = dataDir.resolve("sources");

    var schemaFile = arguments.getString(
      "schema",
      "Location of YML-format schema definition file"
    );

    var path = Path.of(schemaFile);
    SchemaConfig schema;
    if (Files.exists(path)) {
      schema = SchemaConfig.load(path);
    } else {
      // if the file doesn't exist, check if it's bundled in the jar
      schemaFile = schemaFile.startsWith("/samples/") ? schemaFile : "/samples/" + schemaFile;
      if (ConfiguredMapMain.class.getResource(schemaFile) != null) {
        schema = YAML.loadResource(schemaFile, SchemaConfig.class);
      } else {
        throw new IllegalArgumentException("Schema file not found: " + schemaFile);
      }
    }

    // use default argument values from config file as fallback if not set from command-line or env vars
    Contexts.Root rootContext = Contexts.buildRootContext(arguments, schema.args());

    var planetiler = Planetiler.create(rootContext.arguments());
    var profile = new ConfiguredProfile(schema, rootContext);
    planetiler.setProfile(profile);

    for (var source : profile.sources()) {
      configureSource(planetiler, sourcesDir, source);
    }

    planetiler.overwriteOutput("mbtiles", Path.of("data", "output.mbtiles")).run();
  }

  private static void configureSource(Planetiler planetiler, Path sourcesDir, Source source) {

    DataSourceType sourceType = source.type();
    Path localPath = source.localPath();
    if (localPath == null) {
      if (source.url() == null) {
        throw new ParseException("Must provide either a url or path for " + source.id());
      }
      localPath = sourcesDir.resolve(source.defaultFileUrl());
    }

    switch (sourceType) {
      case OSM -> planetiler.addOsmSource(source.id(), localPath, source.url());
      case SHAPEFILE -> planetiler.addShapefileSource(source.id(), localPath, source.url());
      default -> throw new IllegalArgumentException("Unhandled source type for " + source.id() + ": " + sourceType);
    }
  }
}
