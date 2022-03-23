package com.onthegomap.planetiler.custommap;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;

import com.onthegomap.planetiler.FeatureCollector;
import com.onthegomap.planetiler.Planetiler;
import com.onthegomap.planetiler.Profile;
import com.onthegomap.planetiler.config.Arguments;
import com.onthegomap.planetiler.custommap.features.WaterArea;
import com.onthegomap.planetiler.custommap.features.Waterway;
import com.onthegomap.planetiler.reader.SourceFeature;

public class CustomProfile implements Profile {

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
    // Planetiler is a convenience wrapper around the lower-level API for the most common use-cases.
    Planetiler.create(args)
      .setProfile(new CustomProfile())
      .addShapefileSource("water_polygons",
        sourcesDir.resolve("water-polygons-split-3857.zip"),
        "https://osmdata.openstreetmap.de/download/water-polygons-split-3857.zip") // override this default with osm_path="path/to/data.osm.pbf"
      .addOsmSource("osm", Path.of("data", "sources", area + ".osm.pbf"), "geofabrik:" + area)
      // override this default with mbtiles="path/to/output.mbtiles"
      .overwriteOutput("mbtiles", Path.of("data", "spartan.mbtiles"))
      .run();
  }

  private static List<CustomFeature> features = Arrays.asList(
    new WaterArea(),
    new Waterway()
  );

  @Override
  public void processFeature(SourceFeature sourceFeature, FeatureCollector featureCollector) {
    features
      .stream()
      .filter(cf -> cf.includeWhen(sourceFeature))
      .forEach(cf -> cf.processFeature(sourceFeature, featureCollector));
  }

  /*
   * Hooks to override metadata values in the output mbtiles file. Only name is required, the rest are optional. Bounds,
   * center, minzoom, maxzoom are set automatically based on input data and planetiler config.
   *
   * See: https://github.com/mapbox/mbtiles-spec/blob/master/1.3/spec.md#metadata)
   */

  @Override
  public String name() {
    return "Spartan Schema";
  }

  @Override
  public String description() {
    return "Simple vector tile layer";
  }

  @Override
  public boolean isOverlay() {
    return true;//?
  }

  /*
   * Any time you use OpenStreetMap data, you must ensure clients display the following copyright. Most clients will
   * display this automatically if you populate it in the attribution metadata in the mbtiles file:
   */
  @Override
  public String attribution() {
    return """
      <a href="https://www.openstreetmap.org/copyright" target="_blank">&copy; OpenStreetMap contributors</a>
      """.trim();
  }

}
