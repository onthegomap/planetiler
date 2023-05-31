package com.onthegomap.planetiler.examples;

import static com.onthegomap.planetiler.util.MemoryEstimator.CLASS_HEADER_BYTES;

import com.onthegomap.planetiler.FeatureCollector;
import com.onthegomap.planetiler.FeatureMerge;
import com.onthegomap.planetiler.Planetiler;
import com.onthegomap.planetiler.Profile;
import com.onthegomap.planetiler.VectorTile;
import com.onthegomap.planetiler.config.Arguments;
import com.onthegomap.planetiler.geo.GeometryException;
import com.onthegomap.planetiler.reader.SourceFeature;
import com.onthegomap.planetiler.reader.osm.OsmElement;
import com.onthegomap.planetiler.reader.osm.OsmRelationInfo;
import com.onthegomap.planetiler.reader.osm.OsmSourceFeature;
import com.onthegomap.planetiler.util.MemoryEstimator;
import java.nio.file.Path;
import java.util.List;
import java.util.function.Consumer;

public class StreetsProfile implements Profile {

  @Override
  public String name() {
    return "Streets GL Profile";
  }

  private static void processPoint(SourceFeature sourceFeature, FeatureCollector features) {
    if (sourceFeature.hasTag("natural", "tree")) {
      var feature = features.point("point")
        .setAttr("type", "tree")
        .setAttr("leafType", sourceFeature.getTag("leaf_type"))
        .setAttr("genus", sourceFeature.getTag("genus"))
        .setAttr("height", StreetsUtils.getHeight(sourceFeature))
        .setAttr("minHeight", StreetsUtils.getMinHeight(sourceFeature));

      setCommonFeatureParams(feature, sourceFeature);
      return;
    }

    if (StreetsUtils.isFireHydrant(sourceFeature)) {
      var feature = features.point("point")
        .setAttr("type", "fireHydrant")
        .setAttr("height", StreetsUtils.getHeight(sourceFeature))
        .setAttr("minHeight", StreetsUtils.getMinHeight(sourceFeature));

      setCommonFeatureParams(feature, sourceFeature);
      return;
    }

    if (sourceFeature.hasTag("advertising", "column")) {
      var feature = features.point("point")
        .setAttr("type", "adColumn")
        .setAttr("height", StreetsUtils.getHeight(sourceFeature))
        .setAttr("minHeight", StreetsUtils.getMinHeight(sourceFeature));

      setCommonFeatureParams(feature, sourceFeature);
      return;
    }

    if (StreetsUtils.isMemorial(sourceFeature)) {
      var feature = features.point("point")
        .setAttr("type", "adColumn")
        .setAttr("height", StreetsUtils.getHeight(sourceFeature))
        .setAttr("minHeight", StreetsUtils.getMinHeight(sourceFeature));

      setCommonFeatureParams(feature, sourceFeature);
      return;
    }

    if (StreetsUtils.isStatue(sourceFeature)) {
      var feature = features.point("point")
        .setAttr("type", "statue")
        .setAttr("height", StreetsUtils.getHeight(sourceFeature))
        .setAttr("minHeight", StreetsUtils.getMinHeight(sourceFeature));

      setCommonFeatureParams(feature, sourceFeature);
      return;
    }

    if (StreetsUtils.isSculpture(sourceFeature)) {
      var feature = features.point("point")
        .setAttr("type", "sculpture")
        .setAttr("height", StreetsUtils.getHeight(sourceFeature))
        .setAttr("minHeight", StreetsUtils.getMinHeight(sourceFeature));

      setCommonFeatureParams(feature, sourceFeature);
      return;
    }

    if (StreetsUtils.isWindTurbine(sourceFeature)) {
      var feature = features.point("point")
        .setAttr("type", "windTurbine")
        .setAttr("height", StreetsUtils.getHeight(sourceFeature))
        .setAttr("minHeight", StreetsUtils.getMinHeight(sourceFeature));

      setCommonFeatureParams(feature, sourceFeature);
      return;
    }

    if (sourceFeature.hasTag("amenity", "bench")) {
      var feature = features.point("point")
        .setAttr("type", "bench")
        .setAttr("minHeight", StreetsUtils.getMinHeight(sourceFeature))
        .setAttr("direction", StreetsUtils.getDirection(sourceFeature));

      setCommonFeatureParams(feature, sourceFeature);
      return;
    }

    if (sourceFeature.hasTag("leisure", "picnic_table")) {
      var feature = features.point("point")
        .setAttr("type", "picnicTable")
        .setAttr("minHeight", StreetsUtils.getMinHeight(sourceFeature))
        .setAttr("direction", StreetsUtils.getDirection(sourceFeature));

      setCommonFeatureParams(feature, sourceFeature);
      return;
    }

    if (sourceFeature.hasTag("highway", "turning_circle")) {
      var feature = features.point("point").setAttr("type", "roundabout");

      setCommonFeatureParams(feature, sourceFeature);
      return;
    }

    if (sourceFeature.hasTag("highway", "bus_stop")) {
      var feature = features.point("point").setAttr("type", "busStop")
        .setAttr("minHeight", StreetsUtils.getMinHeight(sourceFeature));

      setCommonFeatureParams(feature, sourceFeature);
      return;
    }

    if (sourceFeature.hasTag("aeroway", "helipad")) {
      var feature = features.point("point").setAttr("type", "helipad");

      setCommonFeatureParams(feature, sourceFeature);
      return;
    }

    if (sourceFeature.hasTag("natural", "rock")) {
      var feature = features.point("point").setAttr("type", "rock")
        .setAttr("minHeight", StreetsUtils.getMinHeight(sourceFeature));

      setCommonFeatureParams(feature, sourceFeature);
      return;
    }

    if (sourceFeature.hasTag("power", "pole")) {
      var feature = features.point("point").setAttr("type", "utilityPole")
        .setAttr("minHeight", StreetsUtils.getMinHeight(sourceFeature));

      setCommonFeatureParams(feature, sourceFeature);
      return;
    }

    if (sourceFeature.hasTag("power", "tower")) {
      var feature = features.point("point").setAttr("type", "transmissionTower")
        .setAttr("minHeight", StreetsUtils.getMinHeight(sourceFeature));

      setCommonFeatureParams(feature, sourceFeature);
      return;
    }
  }

  private static void processLine(SourceFeature sourceFeature, FeatureCollector features) {
    if (sourceFeature.hasTag("highway")) {
      var lanes = StreetsUtils.getRoadwayLanes(sourceFeature);

      var feature = features.line("highways")
        .setAttr("type", "path")
        .setAttr("pathType", sourceFeature.getTag("highway"))
        .setAttr("surface", sourceFeature.getTag("surface"))
        .setAttr("width", StreetsUtils.getWidth(sourceFeature))
        .setAttr("laneMarkings", sourceFeature.getTag("lane_markings"))
        .setAttr("sidewalkSide", StreetsUtils.getSidewalkSide(sourceFeature))
        .setAttr("cyclewaySide", StreetsUtils.getCyclewaySide(sourceFeature))
        .setAttr("isOneway", StreetsUtils.isRoadwayOneway(sourceFeature) ? true : null)
        .setAttr("lanesForward", lanes.forward)
        .setAttr("lanesBackward", lanes.backward);

      setCommonFeatureParams(feature, sourceFeature);
      return;
    }

    if (sourceFeature.hasTag("aeroway", "runway") || sourceFeature.hasTag("aeroway", "taxiway")) {
      var feature = features.line("highways")
        .setAttr("type", "path")
        .setAttr("pathType", sourceFeature.getTag("aeroway"))
        .setAttr("width", StreetsUtils.getWidth(sourceFeature));

      setCommonFeatureParams(feature, sourceFeature);
      return;
    }

    if (StreetsUtils.isRailway(sourceFeature)) {
      var feature = features.line("highways")
        .setAttr("type", "path")
        .setAttr("pathType", sourceFeature.getTag("railway"))
        .setAttr("width", StreetsUtils.getWidth(sourceFeature))
        .setAttr("gauge", sourceFeature.getTag("gauge"));

      setCommonFeatureParams(feature, sourceFeature);
      return;
    }

    if (sourceFeature.hasTag("barrier", "fence")) {
      var feature = features.line("barriers")
        .setAttr("type", "fence")
        .setAttr("fenceType", sourceFeature.getTag("fence_type"))
        .setAttr("height", StreetsUtils.getHeight(sourceFeature))
        .setAttr("minHeight", StreetsUtils.getMinHeight(sourceFeature));

      setCommonFeatureParams(feature, sourceFeature);
      return;
    }

    if (sourceFeature.hasTag("barrier", "hedge")) {
      var feature = features.line("barriers")
        .setAttr("type", "wall")
        .setAttr("wallType", "hedge")
        .setAttr("height", StreetsUtils.getHeight(sourceFeature))
        .setAttr("minHeight", StreetsUtils.getMinHeight(sourceFeature));

      setCommonFeatureParams(feature, sourceFeature);
      return;
    }

    if (sourceFeature.hasTag("barrier", "wall")) {
      var feature = features.line("barriers")
        .setAttr("type", "wall")
        .setAttr("wallType", sourceFeature.getTag("wall"))
        .setAttr("height", StreetsUtils.getHeight(sourceFeature))
        .setAttr("minHeight", StreetsUtils.getMinHeight(sourceFeature));

      setCommonFeatureParams(feature, sourceFeature);
      return;
    }

    if (sourceFeature.hasTag("power", "line", "minor_line")) {
      var feature = features.line("powerLines")
        .setAttr("type", "powerLine");

      setCommonFeatureParams(feature, sourceFeature);
      return;
    }

    if (sourceFeature.hasTag("natural", "tree_row")) {
      var feature = features.line("natural")
        .setAttr("type", "treeRow")
        .setAttr("leafType", sourceFeature.getTag("leaf_type"))
        .setAttr("genus", sourceFeature.getTag("genus"))
        .setAttr("height", StreetsUtils.getHeight(sourceFeature))
        .setAttr("minHeight", StreetsUtils.getMinHeight(sourceFeature));

      setCommonFeatureParams(feature, sourceFeature);
      return;
    }

    if (sourceFeature.hasTag("waterway")) {
      var feature = features.line("water")
        .setAttr("type", "waterway")
        .setAttr("waterwayType", sourceFeature.getTag("waterway"));

      setCommonFeatureParams(feature, sourceFeature);
    }
  }

  private boolean processArea(SourceFeature sourceFeature, FeatureCollector features) {
    if (StreetsUtils.isWater(sourceFeature)) {
      var feature = features.polygon("water")
        .setAttr("type", "water");

      setWaterFeatureParams(feature, sourceFeature);
      return true;
    }

    if (
      (
        sourceFeature.hasTag("building:part") &&
          !sourceFeature.getTag("building:part").equals("no")
      ) || (
        sourceFeature.hasTag("building") &&
          !sourceFeature.getTag("building").equals("no") &&
          !this.isBuildingOutline(sourceFeature)
      )
    ) {
      Boolean isPart = sourceFeature.hasTag("building:part");

      var feature = features.polygon("buildings")
        .setAttr("type", "building")
        .setAttr("buildingType", sourceFeature.getTag("building"))
        .setAttr("height", StreetsUtils.getHeight(sourceFeature))
        .setAttr("minHeight", StreetsUtils.getMinHeight(sourceFeature))
        .setAttr("roofHeight", StreetsUtils.getRoofHeight(sourceFeature))
        .setAttr("buildingLevels", StreetsUtils.getBuildingLevels(sourceFeature))
        .setAttr("roofLevels", StreetsUtils.getRoofLevels(sourceFeature))
        .setAttr("roofMaterial", sourceFeature.getTag("roof:material"))
        .setAttr("roofType", sourceFeature.getTag("roof:shape"))
        .setAttr("roofOrientation", sourceFeature.getTag("roof:orientation"))
        .setAttr("roofDirection", StreetsUtils.getRoofDirection(sourceFeature))
        .setAttr("roofAngle", StreetsUtils.getAngle(sourceFeature))
        .setAttr("roofColor", sourceFeature.getTag("roof:colour"))
        .setAttr("color", sourceFeature.getTag("building:colour"))
        .setAttr("noWindows", StreetsUtils.isBuildingHasWindows(sourceFeature, isPart) ? null : true)
        .setBufferPixels(256);

      setCommonFeatureParams(feature, sourceFeature);
    }

    if (sourceFeature.hasTag("area:highway")) {
      var feature = features.polygon("highways")
        .setAttr("type", sourceFeature.getTag("area:highway"));

      setCommonFeatureParams(feature, sourceFeature);
      return true;
    }

    if (sourceFeature.hasTag("highway") && sourceFeature.hasTag("area", "yes")) {
      var feature = features.polygon("highways")
        .setAttr("type", sourceFeature.getTag("highway"));

      setCommonFeatureParams(feature, sourceFeature);
      return true;
    }

    if (sourceFeature.hasTag("landuse", "brownfield")) {
      var feature = features.polygon("common")
        .setAttr("type", "brownfield");

      setCommonFeatureParams(feature, sourceFeature);
      return true;
    }

    if (sourceFeature.hasTag("landuse", "construction")) {
      var feature = features.polygon("common")
        .setAttr("type", "construction");

      setCommonFeatureParams(feature, sourceFeature);
      return true;
    }

    if (sourceFeature.hasTag("landuse", "grass")) {
      var feature = features.polygon("common")
        .setAttr("type", "grass");

      setCommonFeatureParams(feature, sourceFeature);
      return true;
    }

    if (sourceFeature.hasTag("landuse", "farmland")) {
      var feature = features.polygon("common")
        .setAttr("type", "farmland");

      setCommonFeatureParams(feature, sourceFeature);
      return true;
    }

    if (sourceFeature.hasTag("natural", "scrub")) {
      var feature = features.polygon("natural")
        .setAttr("type", "scrub");

      setCommonFeatureParams(feature, sourceFeature);
      return true;
    }

    if (sourceFeature.hasTag("natural", "wood", "forest")) {
      var feature = features.polygon("natural")
        .setAttr("type", "forest");

      setCommonFeatureParams(feature, sourceFeature);
      return true;
    }

    if (sourceFeature.hasTag("natural", "sand", "beach")) {
      var feature = features.polygon("natural")
        .setAttr("type", "sand");

      setCommonFeatureParams(feature, sourceFeature);
      return true;
    }

    if (sourceFeature.hasTag("natural", "rock", "bare_rock")) {
      var feature = features.polygon("natural")
        .setAttr("type", "rock");

      setCommonFeatureParams(feature, sourceFeature);
      return true;
    }

    if (sourceFeature.hasTag("leisure", "pitch")) {
      var feature = features.polygon("common")
        .setAttr("type", "pitch")
        .setAttr("pitchType", sourceFeature.getTag("sport"))
        .setAttr("surface", sourceFeature.getTag("surface"));

      setCommonFeatureParams(feature, sourceFeature);
      return true;
    }

    if (sourceFeature.hasTag("leisure", "playground")) {
      var feature = features.polygon("common")
        .setAttr("type", "playground");

      setCommonFeatureParams(feature, sourceFeature);
      return true;
    }

    if (sourceFeature.hasTag("leisure", "dog_park")) {
      var feature = features.polygon("common")
        .setAttr("type", "dogPark");

      setCommonFeatureParams(feature, sourceFeature);
      return true;
    }

    if (
      sourceFeature.hasTag("leisure", "garden") ||
      sourceFeature.hasTag("landuse", "flowerbed")
    ) {
      var feature = features.polygon("common")
        .setAttr("type", "garden");

      setCommonFeatureParams(feature, sourceFeature);
      return true;
    }

    if (sourceFeature.hasTag("golf", "fairway")) {
      var feature = features.polygon("common")
        .setAttr("type", "fairway");

      setCommonFeatureParams(feature, sourceFeature);
      return true;
    }

    if (
      sourceFeature.hasTag("amenity", "parking", "bicycle_parking") &&
        (sourceFeature.hasTag("parking", "surface") || sourceFeature.getTag("parking") == null)
    ) {
      var feature = features.polygon("common")
        .setAttr("type", "parking")
        .setAttr("surface", sourceFeature.getTag("surface"));

      setCommonFeatureParams(feature, sourceFeature);
      return true;
    }

    if (sourceFeature.hasTag("man_made", "bridge")) {
      var feature = features.polygon("common")
        .setAttr("type", "bridge");

      setCommonFeatureParams(feature, sourceFeature);
      return true;
    }

    if (sourceFeature.hasTag("man_made", "apron")) {
      var feature = features.polygon("common")
        .setAttr("type", "apron");

      setCommonFeatureParams(feature, sourceFeature);
      return true;
    }

    if (sourceFeature.hasTag("aeroway", "helipad")) {
      var feature = features.polygon("common")
        .setAttr("type", "helipad");

      setCommonFeatureParams(feature, sourceFeature);
      return true;
    }

    return false;
  }

  @Override
  public void processFeature(SourceFeature sourceFeature, FeatureCollector features) {
    if (StreetsUtils.isUnderground(sourceFeature)) {
      return;
    }

    if (sourceFeature.canBePolygon()) {
      boolean wasProcessed = processArea(sourceFeature, features);

      if (wasProcessed) {
        return;
      }
    }

    if (sourceFeature.canBeLine()) {
      processLine(sourceFeature, features);
      return;
    }

    if (sourceFeature.isPoint()) {
      processPoint(sourceFeature, features);
    }
  }

  private static void setCommonFeatureParams(FeatureCollector.Feature feature, SourceFeature sourceFeature) {
    if (sourceFeature instanceof OsmSourceFeature osmFeature) {
      OsmElement element = osmFeature.originalElement();

      feature
        .setAttr("osmId", sourceFeature.id())
        .setAttr("osmType", element instanceof OsmElement.Node ? 0 :
          element instanceof OsmElement.Way ? 1 :
            element instanceof OsmElement.Relation ? 2 : null
        );
    }

    feature
      .setZoomRange(16, 16)
      .setPixelToleranceAtAllZooms(0)
      .setMinPixelSize(0);
  }

  private static void setWaterFeatureParams(FeatureCollector.Feature feature, SourceFeature sourceFeature) {
    feature
      .setZoomRange(16, 16)
      .setPixelToleranceAtAllZooms(0)
      .setMinPixelSize(0);
  }

  public static void main(String[] args) throws Exception {
    run(Arguments.fromArgsOrConfigFile(args));
  }

  public static void run(Arguments args) throws Exception {
    Planetiler.create(args)
      .setProfile(new StreetsProfile())
      .addOsmSource("osm", Path.of("data", "sources", "nyc.osm.pbf"))
      .addShapefileSource("water", Path.of("data", "sources", "water_polygons", "water_polygons.shp"))
      .overwriteOutput(Path.of("data", "test.mbtiles"))
      .run();
  }

  @Override
  public void finish(String name, FeatureCollector.Factory featureCollectors, Consumer<FeatureCollector.Feature> next) {
    System.out.println("Finished");
  }

  @Override
  public List<VectorTile.Feature> postProcessLayerFeatures(String layer, int zoom, List<VectorTile.Feature> items) throws GeometryException {
    if (!layer.equals("water")) {
      return items;
    }

    if (items.size() > 1) {
      return FeatureMerge.mergeOverlappingPolygons(items, 0);
    }

    return items;
  }

  @Override
  public List<OsmRelationInfo> preprocessOsmRelation(OsmElement.Relation relation) {
    if (relation.hasTag("type", "building")) {
      return List.of(new BuildingRelationInfo(relation.id()));
    }

    return null;
  }

  private record BuildingRelationInfo(long id) implements OsmRelationInfo {
    @Override
    public long estimateMemoryUsageBytes() {
      return CLASS_HEADER_BYTES + MemoryEstimator.estimateSizeLong(id);
    }
  }

  private boolean isBuildingOutline(SourceFeature sourceFeature) {
    var relations = sourceFeature.relationInfo(BuildingRelationInfo.class);

    for (var relation : relations) {
      if ("outline".equals(relation.role())) {
        return true;
      }
    }

    return false;
  }
}
