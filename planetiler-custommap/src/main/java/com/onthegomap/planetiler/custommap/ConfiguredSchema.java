package com.onthegomap.planetiler.custommap;

import com.onthegomap.planetiler.Planetiler;
import com.onthegomap.planetiler.config.Arguments;
import com.onthegomap.planetiler.custommap.configschema.DataSource;
import com.onthegomap.planetiler.custommap.configschema.DataSourceType;
import com.onthegomap.planetiler.custommap.configschema.SchemaConfig;
import java.io.File;
import java.io.FileInputStream;
import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import org.yaml.snakeyaml.Yaml;

public class ConfiguredSchema {

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
        Paths.get("samples", "owg_simple.yml").toString());

    Yaml yml = new Yaml();
    SchemaConfig config = yml.loadAs(new FileInputStream(new File(schemaFile)), SchemaConfig.class);

    Planetiler planetiler = Planetiler.create(args)
      .setProfile(new ConfiguredProfile(config));

    Collection<DataSource> sources = config.getSources();
    for (DataSource source : sources) {
      configureSource(planetiler, sourcesDir, source);
    }

    //TODO move to command-line args
    planetiler.overwriteOutput("mbtiles", Path.of("data", "spartan.mbtiles"))
      .run();
  }

  private static void configureSource(Planetiler planetiler, Path sourcesDir, DataSource source)
    throws Exception {

    DataSourceType sourceType = source.getType();
    String sourceName = source.getName();

    switch (sourceType) {
      case osm:
        String area = source.getArea();
        String[] areaParts = area.split(":");
        String areaName = areaParts[areaParts.length - 1];
        planetiler.addOsmSource(sourceName, sourcesDir.resolve(areaName + ".osm.pbf"), area);
        return;
      case shapefile:
        String url = source.getUrl();
        String filename = Paths.get(new URI(url).getPath()).getFileName().toString();
        planetiler.addShapefileSource(sourceName, sourcesDir.resolve(filename), url);
        return;
      default:
        throw new IllegalArgumentException("Uhandled source " + sourceType);
    }
  }
}
