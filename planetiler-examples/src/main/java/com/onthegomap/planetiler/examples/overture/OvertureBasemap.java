package com.onthegomap.planetiler.examples.overture;

import com.onthegomap.planetiler.FeatureCollector;
import com.onthegomap.planetiler.FeatureMerge;
import com.onthegomap.planetiler.Planetiler;
import com.onthegomap.planetiler.Profile;
import com.onthegomap.planetiler.VectorTile;
import com.onthegomap.planetiler.config.Arguments;
import com.onthegomap.planetiler.geo.GeometryException;
import com.onthegomap.planetiler.reader.SourceFeature;
import com.onthegomap.planetiler.util.Glob;
import java.nio.file.Path;
import java.util.List;

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
      case "land_cover" -> {
        if (source.getLong("cartography.max_zoom") < 8) {
          features.polygon("land_cover")
            .setMinZoom(source.getStruct("cartography", "min_zoom").asInt())
            .setMaxZoom(source.getStruct("cartography", "max_zoom").asInt())
            .inheritAttrFromSource("subtype")
            .setMinPixelSize(0);
        }
      }
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

  @Override
  public List<VectorTile.Feature> postProcessLayerFeatures(String layer, int zoom, List<VectorTile.Feature> items)
    throws GeometryException {
    return FeatureMerge.mergeOverlappingPolygons(items, 0.1);
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
      .addParquetSource("overture-landcover",
        Glob.of(base).resolve("theme=base", "type=land_cover", "*.parquet").find(),
        true, fields -> fields.get("id"), fields -> fields.get("type"))
      .overwriteOutput(Path.of("data", "overture.pmtiles"))
      .run();
  }
}
