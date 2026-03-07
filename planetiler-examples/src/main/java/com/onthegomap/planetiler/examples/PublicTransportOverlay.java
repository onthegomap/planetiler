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

public class PublicTransportOverlay implements Profile {

  // This record stores a tram route's tags
  private record RouteRelationInfo(
    @Override long id,
    String ref,
    String name,
    String network,
    String colour,
    String to,
    String from
  ) implements OsmRelationInfo {}

  @Override
  public List<OsmRelationInfo> preprocessOsmRelation(OsmElement.Relation relation) {
    // For routes of type tram
    if (relation.hasTag("type", "route")) {
      if (relation.hasTag("route", "tram")) {
        // Form the route as record and add it to the relation list
        return List.of(new RouteRelationInfo(
          relation.id(),
          relation.getString("ref"),
          relation.getString("name"),
          relation.getString("network"),
          relation.getString("colour"),
          relation.getString("to"),
          relation.getString("from")
        ));
      }
    }
    // Return null for any relation that isn't of a tram route
    return null;
  }

  // For now, create an overlay that displays tram lines and their stops
  @Override
  public void processFeature(SourceFeature sourceFeature, FeatureCollector features) {
    // Collect tram stop features
    /*
    if (sourceFeature.isPoint() && sourceFeature.hasTag("railway", "tram_stop")) {
      features.point("Tram stop")
        .setAttr("name", sourceFeature.getTag("name"));
    }
     */
    if (sourceFeature.canBeLine()) {
      for (var routeInfo : sourceFeature.relationInfo(RouteRelationInfo.class, true)) {
        RouteRelationInfo relation = routeInfo.relation();
        // Name each layer as {network}:{name}-from-{start station}-to-{destination station}
        String layerName = relation.network + ":" + relation.name + "-from-" + relation.from + "-to-" + relation.to;
        features.line(layerName)
          .setAttr("ref", relation.ref)
          .setAttr("network", relation.network)
          .setAttr("name", relation.name);
        //.setZoomRange(0, 14)
        //.setMinPixelSize(0);
      }
    }
  }

  // Merge lines at their endpoints for improved rendering
  public List<VectorTile.Feature> postProcessLayerFeatures(String layer, int zoom, List<VectorTile.Feature> items) {
    return FeatureMerge.mergeLineStrings(items, 0.5, 0.1, 4);
  }

  @Override
  public boolean isOverlay() {
    return true;
  }

  @Override
  public String name() {
    // name that shows up in the MBTiles metadata table
    return "Public Transport Overlay";
  }

  @Override
  public String description() {
    return "An example overlay that shows tram routes and stops";
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
      .setProfile(new PublicTransportOverlay())
      // if input.pbf not found, download Berlin from Geofabrik
      .addOsmSource("osm", Path.of("data", "sources", area + ".osm.pbf"), "geofabrik:" + area)
      .overwriteOutput(Path.of("data", "publictransport.mbtiles"))
      .run();
  }
}
