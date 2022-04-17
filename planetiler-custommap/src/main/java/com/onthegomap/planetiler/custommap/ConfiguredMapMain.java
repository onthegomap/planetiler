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

public class ConfiguredMapMain {

  /*
   * Main entrypoint
   */
  public static void main(String[] args) throws Exception {
    run(Arguments.fromArgsOrConfigFile(args));
  }

  private static void run(Arguments args) throws Exception {
    Path dataDir = Path.of("data");
    Path sourcesDir = dataDir.resolve("sources");

    String schemaFile = args.getString(
      "schema",
      "Location of YML-format schema definition file",
      "");

    if (schemaFile.isEmpty()) {
      System.out.println("Schema not specified; use --schema=schema_file_name.yml");
      return;
    }

    Yaml yml = new Yaml();
    SchemaConfig config = yml.loadAs(new FileInputStream(new File(schemaFile)), SchemaConfig.class);

    Planetiler planetiler = Planetiler.create(args)
      .setProfile(new ConfiguredProfile(config));

    Collection<DataSource> sources = config.getSources();
    for (DataSource source : sources) {
      configureSource(planetiler, sourcesDir, source);
    }

    planetiler.overwriteOutput("mbtiles", Path.of("data", "output.mbtiles"))
      .run();
  }

  private static void configureSource(Planetiler planetiler, Path sourcesDir, DataSource source)
    throws Exception {

    DataSourceType sourceType = source.getType();
    String sourceName = source.getName();

    switch (sourceType) {
      case osm:
        String area = source.getUrl();
        String[] areaParts = area.split("[:/]");
        String areaFilename = areaParts[areaParts.length - 1];
        String areaName = areaFilename.replaceAll("\\..*?$", "");
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
