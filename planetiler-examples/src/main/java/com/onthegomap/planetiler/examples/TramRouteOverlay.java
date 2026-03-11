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
import java.util.Collections;
import java.util.List;

/**
 * This example profile builds a map of tram routes from OpenStreetMap relations tagged with
 * <a href="https://wiki.openstreetmap.org/wiki/Tag:route%3Dtram">route=tram</a>
 * as well as tram stop nodes tagged with
 * <a href="https://wiki.openstreetmap.org/wiki/Tag:railway%3Dtram_stop">railway=tram_stop</a>
 * and tram ways tagged with
 * <a href="https://wiki.openstreetmap.org/wiki/Tag:railway%3Dtram">railway=tram</a>.
 * <p>
 * To run this example:
 * <ol>
 * <li>Download an .osm.pbf extract (see the <a href="https://download.geofabrik.de/">Geofabrik download site</a>)</li>
 * <li>build the example: {@code mvn clean package --file standalone.pom.xml}</li>
 * <li>run this example:
 * {@code java -cp target/*-with-deps.jar com.onthegomap.planetiler.examples.TramRouteOverlay osm_path="path/to/data.osm.pbf" mbtiles="data/tramroutes.mbtiles"}</li>
 * <li>then run the demo tileserver: {@code tileserver-gl data/tramroutes.mbtiles}</li>
 * <li>view the output at <a href="http://localhost:8080">localhost:8080</a></li>
 * </ol>
 */
public class TramRouteOverlay implements Profile {

  // Data carrier to store a tram route's attributes
  private record RouteRelationInfo(
    @Override long id,
    String ref,
    String name,
    String network
  ) implements OsmRelationInfo {}

  // Pack a route relation into a list object before feature processing
  @Override
  public List<OsmRelationInfo> preprocessOsmRelation(OsmElement.Relation relation) {
    if (relation.hasTag("type", "route") && relation.hasTag("route", "tram")) {
      // Form the route as a record, then return it as a relation list
      return List.of(new RouteRelationInfo(
        relation.id(),
        relation.getString("ref"),
        relation.getString("name"),
        relation.getString("network")
      ));
    }
    // Return an empty list for any relation that is not a tram route
    return Collections.emptyList();
  }

  @Override
  public void processFeature(SourceFeature sourceFeature, FeatureCollector features) {
    // Process tram route relations
    if (sourceFeature.canBeLine()) {
      for (var routeInfo : sourceFeature.relationInfo(RouteRelationInfo.class, true)) {
        RouteRelationInfo relation = routeInfo.relation();
        String layerName = relation.name;
        features.line(layerName)
          .setAttr("ref", relation.ref)
          .setAttr("network", relation.network)
          .setAttr("name", relation.name)
          .setZoomRange(0, 20)
          .setMinPixelSize(0); // Prevents visual gaps in tram routes
      }
    }

    // Process tram ways that are not included in their respective tram route relation
    if (sourceFeature.canBeLine() && sourceFeature.hasTag("railway", "tram")) {
      features.line("tram")
        .setAttr("ref", sourceFeature.getTag("ref"))
        .setZoomRange(0, 20)
        .setMinPixelSize(0);
    }

    // Process tram stop nodes
    if (sourceFeature.isPoint() && sourceFeature.hasTag("railway", "tram_stop")) {
      features.point("tram_stop")
        .setAttr("name", sourceFeature.getTag("name"))
        .setMinZoom(11); // Prevent tram stops from cluttering routes when zoomed far out
    }
  }

  // Merge lines at their endpoints for improved rendering. Tram routes will otherwise appear less accurately
  @Override
  public List<VectorTile.Feature> postProcessLayerFeatures(String layer, int zoom, List<VectorTile.Feature> items) {
    return FeatureMerge.mergeLineStrings(
      items,
      0.5, // remove lines that are less than 0.5px long
      0.1, // simplify linestring output with a 0.1px tolerance
      4 // remove detail more than 4px outside the tile boundary
    );
  }

  @Override
  public boolean isOverlay() {
    return true;
  }

  @Override
  public String name() {
    // The name that appears in the MBTiles metadata table
    return "Tram Routes and Stops Overlay";
  }

  @Override
  public String description() {
    return "This example overlay shows tram routes with their stops";
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

  public static void main(String[] args) throws Exception {
    run(Arguments.fromArgsOrConfigFile(args));
  }

  static void run(Arguments args) throws Exception {
    String area = args.getString("area", "geofabrik area to download", "bremen");
    Planetiler.create(args)
      .setProfile(new TramRouteOverlay())
      // if the argument osm_path="path/to/data.osm.pbf" is not given, download Bremen from Geofabrik
      .addOsmSource("osm", Path.of("data", "sources", area + ".osm.pbf"), "geofabrik:" + area)
      .overwriteOutput(Path.of("data", "tramroutes.mbtiles"))
      .run();
  }
}
