package com.onthegomap.planetiler.examples;

import com.onthegomap.planetiler.FeatureCollector;
import com.onthegomap.planetiler.FeatureMerge;
import com.onthegomap.planetiler.Planetiler;
import com.onthegomap.planetiler.Profile;
import com.onthegomap.planetiler.VectorTile;
import com.onthegomap.planetiler.config.Arguments;
import com.onthegomap.planetiler.reader.SourceFeature;
import com.onthegomap.planetiler.reader.osm.OsmElement;
import com.onthegomap.planetiler.reader.osm.OsmRelationInfo;
import java.nio.file.Path;
import java.util.List;

/**
 * Builds a map of bike routes from ways contained in OpenStreetMap relations tagged with
 * <a href="https://wiki.openstreetmap.org/wiki/Tag:route=bicycle">route=bicycle</a>.
 * <p>
 * To run this example:
 * <ol>
 * <li>Download a .osm.pbf extract (see <a href="https://download.geofabrik.de/">Geofabrik download site</a>)</li>
 * <li>then build the examples: {@code mvn clean package}</li>
 * <li>then run this example:
 * {@code java -cp target/*-with-deps.jar com.onthegomap.planetiler.examples.BikeRouteOverlay osm_path="path/to/data.osm.pbf" mbtiles="data/output.mbtiles"}</li>
 * <li>then run the demo tileserver: {@code tileserver-gl-light --mbtiles data/bikeroutes.mbtiles}</li>
 * <li>and view the output at <a href="http://localhost:8080">localhost:8080</a></li>
 * </ol>
 */
public class BikeRouteOverlay implements Profile {
  /*
   * The processing happens in 4 steps:
   * 1. On the first pass through the input file, store relevant information from OSM bike route relations
   * 2. On the second pass, emit linestrings for each OSM way contained in one of those relations
   * 3. Before storing each finished tile, Merge linestrings in each tile with the same tags and touching endpoints
   */

  /*
   * Step 1)
   *
   * Planetiler processes the .osm.pbf input file in two passes. The first pass stores node locations, and invokes
   * preprocessOsmRelation for reach relation and stores information the profile needs during the second pass when we
   * emit map feature for ways contained in that relation.
   */

  // Minimal container for data we extract from OSM bicycle route relations. This is held in RAM so keep it small.
  private record RouteRelationInfo(
    // OSM ID of the relation (required):
    @Override long id,
    // Values for tags extracted from the OSM relation:
    String name, String ref, String route, String network
  ) implements OsmRelationInfo {}


  @Override
  public List<OsmRelationInfo> preprocessOsmRelation(OsmElement.Relation relation) {
    // If this is a "route" relation ...
    if (relation.hasTag("type", "route")) {
      // where route=bicycle or route=mtb ...
      if (relation.hasTag("route", "mtb", "bicycle")) {
        // then store a RouteRelationInfo instance with tags we'll need later
        return List.of(new RouteRelationInfo(
          relation.id(),
          relation.getString("name"),
          relation.getString("ref"),
          relation.getString("route"),
          // except map network abbreviation to a human-readable value
          switch (relation.getString("network", "")) {
            case "icn" -> "international";
            case "ncn" -> "national";
            case "rcn" -> "regional";
            case "lcn" -> "local";
            default -> "other";
          }
        ));
      }
    }
    // for any other relation, return null to ignore
    return null;
  }

  /*
   * Step 2)
   *
   * On the second pass through the input .osm.pbf file, for each way in a relation that we stored data about, emit a
   * linestring map feature with attributes derived from the relation.
   */

  @Override
  public void processFeature(SourceFeature sourceFeature, FeatureCollector features) {
    // ignore nodes and ways that should only be treated as polygons
    if (sourceFeature.canBeLine()) {
      // get all the RouteRelationInfo instances we returned from preprocessOsmRelation that
      // this way belongs to
      for (var routeInfo : sourceFeature.relationInfo(RouteRelationInfo.class)) {
        // (routeInfo.role() also has the "role" of this relation member if needed)
        RouteRelationInfo relation = routeInfo.relation();
        // Break the output into layers named: "{bicycle,route}-route-{international,national,regional,local,other}"
        String layerName = relation.route + "-route-" + relation.network;
        features.line(layerName)
          .setAttr("name", relation.name)
          .setAttr("ref", relation.ref)
          .setZoomRange(0, 14)
          // don't filter out short line segments even at low zooms because the next step needs them
          // to merge lines with the same tags where the endpoints are touching
          .setMinPixelSize(0);
      }
    }
  }

  /*
   * Step 3)
   *
   * Before writing tiles to the output, first merge linestrings where the endpoints are touching that share the same
   * tags to improve line and text rendering in clients.
   */

  @Override
  public List<VectorTile.Feature> postProcessLayerFeatures(String layer, int zoom,
    List<VectorTile.Feature> items) {
    // FeatureMerge has several utilities for merging geometries in a layer that share the same tags.
    // `mergeLineStrings` combines lines with the same tags where the endpoints touch.
    // Tiles are 256x256 pixels and all FeatureMerge operations work in tile pixel coordinates.
    return FeatureMerge.mergeLineStrings(items,
      0.5, // after merging, remove lines that are still less than 0.5px long
      0.1, // simplify output linestrings using a 0.1px tolerance
      4 // remove any detail more than 4px outside the tile boundary
    );
  }

  /*
   * Hooks to override metadata values in the output mbtiles file. Only name is required, the rest are optional. Bounds,
   * center, minzoom, maxzoom are set automatically based on input data and planetiler config.
   *
   * See: https://github.com/mapbox/mbtiles-spec/blob/master/1.3/spec.md#metadata)
   */

  @Override
  public String name() {
    return "Bike Paths Overlay";
  }

  @Override
  public String description() {
    return "An example overlay showing bicycle routes";
  }

  @Override
  public boolean isOverlay() {
    return true; // when true sets type=overlay, otherwise type=baselayer
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
   * Main entrypoint for this example program
   */
  public static void main(String[] args) throws Exception {
    run(Arguments.fromArgsOrConfigFile(args));
  }

  static void run(Arguments args) throws Exception {
    String area = args.getString("area", "geofabrik area to download", "monaco");
    // Planetiler is a convenience wrapper around the lower-level API for the most common use-cases.
    // See ToiletsOverlayLowLevelApi for an example using the lower-level API
    Planetiler.create(args)
      .setProfile(new BikeRouteOverlay())
      // override this default with osm_path="path/to/data.osm.pbf"
      .addOsmSource("osm", Path.of("data", "sources", area + ".osm.pbf"), "geofabrik:" + area)
      // override this default with mbtiles="path/to/output.mbtiles"
      .overwriteOutput("mbtiles", Path.of("data", "bikeroutes.mbtiles"))
      .run();
  }
}
