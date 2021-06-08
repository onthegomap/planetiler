package com.onthegomap.flatmap.examples;

import com.graphhopper.reader.ReaderRelation;
import com.onthegomap.flatmap.FeatureCollector;
import com.onthegomap.flatmap.FeatureMerge;
import com.onthegomap.flatmap.FlatMapRunner;
import com.onthegomap.flatmap.Profile;
import com.onthegomap.flatmap.SourceFeature;
import com.onthegomap.flatmap.VectorTileEncoder;
import com.onthegomap.flatmap.geo.GeometryException;
import com.onthegomap.flatmap.read.OpenStreetMapReader;
import java.nio.file.Path;
import java.util.List;

public class BikeRouteOverlay implements Profile {

  private static record RouteRelationInfo(String name, String ref, String route, String network) implements
    OpenStreetMapReader.RelationInfo {}

  @Override
  public List<OpenStreetMapReader.RelationInfo> preprocessOsmRelation(ReaderRelation relation) {
    if (relation.hasTag("type", "route")) {
      String type = relation.getTag("route");
      if ("mtb".equals(type) || "bicycle".equals(type)) {
        return List.of(new RouteRelationInfo(
          relation.getTag("name"),
          relation.getTag("ref"),
          type,
          switch (relation.getTag("network", "")) {
            case "icn" -> "international";
            case "ncn" -> "national";
            case "rcn" -> "regional";
            case "lcn" -> "local";
            default -> "other";
          }
        ));
      }
    }
    return null;
  }

  @Override
  public void processFeature(SourceFeature sourceFeature, FeatureCollector features) {
    if (sourceFeature.canBeLine()) {
      for (RouteRelationInfo routeInfo : sourceFeature.relationInfo(RouteRelationInfo.class)) {
        features.line(routeInfo.route + "-route-" + routeInfo.network)
          .setAttr("name", routeInfo.name)
          .setAttr("ref", routeInfo.ref)
          .setZoomRange(0, 14)
          .setMinPixelSize(0);
      }
    }
  }

  @Override
  public List<VectorTileEncoder.Feature> postProcessLayerFeatures(String layer, int zoom,
    List<VectorTileEncoder.Feature> items) throws GeometryException {
    return FeatureMerge.mergeLineStrings(items, 0.5, 0.1, 4);
  }

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
    return true;
  }

  @Override
  public String attribution() {
    return """
      <a href="https://www.openstreetmap.org/copyright" target="_blank">&copy; OpenStreetMap contributors</a>
      """.trim();
  }

  public static void main(String[] args) throws Exception {
    FlatMapRunner.create()
      .setProfile(new BikeRouteOverlay())
      .addOsmSource("osm", Path.of("data", "sources", "north-america_us_massachusetts.pbf"))
      .overwriteOutput("mbtiles", Path.of("data", "bikeroutes.mbtiles"))
      .run();
  }
}
