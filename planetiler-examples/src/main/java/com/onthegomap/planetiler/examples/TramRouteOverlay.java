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

public class TramRouteOverlay implements Profile {

  // Data carrier to store a tram route's tags
  private record RouteRelationInfo(
    @Override long id,
    String ref,
    String name,
    String network
  ) implements OsmRelationInfo {}

  @Override
  public List<OsmRelationInfo> preprocessOsmRelation(OsmElement.Relation relation) {
    // For routes of type tram
    if (relation.hasTag("type", "route")) {
      if (relation.hasTag("route", "tram")) {
        // Form the route as a record and add it to the relation list
        return List.of(new RouteRelationInfo(
          relation.id(),
          relation.getString("ref"),
          relation.getString("name"),
          relation.getString("network")
        ));
      }
    }
    // Return null for any relation that isn't of a tram route
    return null;
  }

  @Override
  public void processFeature(SourceFeature sourceFeature, FeatureCollector features) {
    // Process tram stop nodes
    if (sourceFeature.isPoint() && sourceFeature.hasTag("railway", "tram_stop")) {
      features.point("tram_stop")
        .setAttr("name", sourceFeature.getTag("name"))
        .setMinZoom(11); // Prevent tram stops from cluttering routes when zoomed far out
    }
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
  }

  // Merge lines at their endpoints for improved rendering
  public List<VectorTile.Feature> postProcessLayerFeatures(String layer, int zoom, List<VectorTile.Feature> items) {
    return FeatureMerge.mergeLineStrings(items,
      0.5, // remove lines that are less than 0.5px long
      0.1, // simplify linestring output using a 0.1px tolerance
      4 // remove detail more than 4px outside the tile boundary
    );
  }

  @Override
  public boolean isOverlay() {
    return true;
  }

  @Override
  public String name() {
    // Name that appears in the MBTiles metadata table
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
    String area = args.getString("area", "geofabrik area to download", "Berlin");
    Planetiler.create(args)
      .setProfile(new TramRouteOverlay())
      // if input.pbf not found, download Berlin from Geofabrik
      .addOsmSource("osm", Path.of("data", "sources", area + ".osm.pbf"), "geofabrik:" + area)
      .overwriteOutput(Path.of("data", "tramroutes.mbtiles"))
      .run();
  }
}
