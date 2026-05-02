package com.onthegomap.planetiler.examples;

import com.onthegomap.planetiler.FeatureCollector;
import com.onthegomap.planetiler.Planetiler;
import com.onthegomap.planetiler.Profile;
import com.onthegomap.planetiler.config.Arguments;
import com.onthegomap.planetiler.reader.SourceFeature;
import java.nio.file.Path;

/**
 * Builds a map of parks and gardens from OpenStreetMap ways and relations tagged with
 * <a href="https://wiki.openstreetmap.org/wiki/Tag:leisure%3Dpark">leisure=park</a> or
 * <a href="https://wiki.openstreetmap.org/wiki/Tag:leisure%3Dgarden">leisure=garden</a>.
 */
public class ParksOverlay implements Profile {

  @Override
  public void processFeature(SourceFeature sourceFeature, FeatureCollector features) {
    if (sourceFeature.canBePolygon() && sourceFeature.hasTag("leisure", "park", "garden")) {
      features.polygon("parks")
        .setZoomRange(4, 14)
        .setAttr("name", sourceFeature.getTag("name"))
        .setAttr("leisure", sourceFeature.getTag("leisure"))
        .setAttr("access", sourceFeature.getTag("access"))
        // drop polygons too small to be visible so low-zoom tiles stay compact
        .setMinPixelSize(2);
    }
  }

  /*
   * Hooks to override metadata values in the output mbtiles file. Only name is required, the rest are optional. Bounds,
   * center, minzoom, maxzoom are set automatically based on input data and planetiler config.
   *
   * See: https://github.com/mapbox/mbtiles-spec/blob/master/1.3/spec.md#metadata)
   */

  @Override
  public String name() {
    return "Parks Overlay";
  }

  @Override
  public String description() {
    return "An example overlay showing parks and gardens";
  }

  @Override
  public boolean isOverlay() {
    return true;
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

  /*
   * Main entrypoint for the example program
   */
  public static void main(String[] args) {
    run(Arguments.fromArgsOrConfigFile(args));
  }

  static void run(Arguments args) {
    String area = args.getString("area", "geofabrik area to download", "monaco");
    // Planetiler is a convenience wrapper around the lower-level API for the most common use-cases.
    Planetiler.create(args)
      .setProfile(new ParksOverlay())
      // override this default with osm_path="path/to/data.osm.pbf"
      .addOsmSource("osm", Path.of("data", "sources", area + ".osm.pbf"), "geofabrik:" + area)
      // override this default with mbtiles="path/to/output.mbtiles"
      .overwriteOutput(Path.of("data", "parks.mbtiles"))
      .run();
  }
}
