package com.onthegomap.planetiler.examples.overture;

import com.onthegomap.planetiler.FeatureCollector;
import com.onthegomap.planetiler.Planetiler;
import com.onthegomap.planetiler.Profile;
import com.onthegomap.planetiler.config.Arguments;
import com.onthegomap.planetiler.reader.SourceFeature;
import com.onthegomap.planetiler.util.Glob;
import java.nio.file.Path;

/**
 * Example basemap using <a href="https://overturemaps.org/">Overture Maps</a> data.
 */
public class OvertureBasemap implements Profile {

  @Override
  public void processFeature(SourceFeature source, FeatureCollector features) {
    String layer = source.getSourceLayer();
    switch (layer) {
      case "building" -> features.polygon("building")
        .setMinZoom(13)
        .inheritAttrFromSource("height")
        .inheritAttrFromSource("roof_color");
      case null, default -> {
        // ignore for now
      }
    }
  }

  @Override
  public String name() {
    return "Overture";
  }

  @Override
  public String description() {
    return "A basemap generated from Overture data";
  }

  @Override
  public String attribution() {
    return """
      <a href="https://www.openstreetmap.org/copyright" target="_blank">&copy; OpenStreetMap</a>
      <a href="https://docs.overturemaps.org/attribution" target="_blank">&copy; Overture Maps Foundation</a>
      """
      .replace("\n", " ")
      .trim();
  }

  public static void main(String[] args) throws Exception {
    run(Arguments.fromArgsOrConfigFile(args));
  }

  static void run(Arguments args) throws Exception {
    Path base = args.inputFile("base", "overture base directory", Path.of("data", "overture"));
    Planetiler.create(args)
      .setProfile(new OvertureBasemap())
      .addParquetSource("overture-buildings",
        Glob.of(base).resolve("*", "type=building", "*.parquet").find(),
        true, // hive-partitioning
        fields -> fields.get("id"), // hash the ID field to generate unique long IDs
        fields -> fields.get("type")) // extract "type={}" from the filename to get layer
      .overwriteOutput(Path.of("data", "overture.pmtiles"))
      .run();
  }
}
