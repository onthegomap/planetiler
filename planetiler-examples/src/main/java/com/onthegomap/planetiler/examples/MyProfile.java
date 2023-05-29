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
import java.util.Arrays;
import java.util.Dictionary;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;
import org.locationtech.jts.geom.MultiPolygon;
import org.locationtech.jts.geom.Polygon;
import com.onthegomap.planetiler.examples.StreetsUtils;

public class MyProfile implements Profile {

  @Override
  public String name() {
    // name that shows up in the MBTiles metadata table
    return "My Profile";
  }

  private static boolean isUnderground(SourceFeature sourceFeature) {
    String layerTag = (String)sourceFeature.getTag("layer");

    if (layerTag != null) {
      float layer = Float.parseFloat(layerTag);

      if (layer < 0) {
        return true;
      }
    }

    return sourceFeature.hasTag("location", "underground") ||
      sourceFeature.hasTag("tunnel", "yes") ||
      sourceFeature.hasTag("parking", "underground");
  }

  private static final List<String> buildingsWithoutWindows = Arrays.asList(
    "garage", "garages", "greenhouse", "storage_tank", "bunker", "silo", "stadium",
    "ship", "castle", "service", "digester", "water_tower", "shed", "ger", "barn",
    "slurry_tank", "container", "carport"
  );

  private static boolean isBuildingHasWindows(SourceFeature sourceFeature, Boolean isPart) {
    String windowValue = (String)sourceFeature.getTag("window");
    String windowsValue = (String)sourceFeature.getTag("windows");

    if (windowValue == "no" || windowsValue == "no") {
      return false;
    }

    if (windowValue == "yes" || windowsValue == "yes") {
      return true;
    }

    if (
      sourceFeature.hasTag("bridge:support") ||
        sourceFeature.hasTag("man_made", "storage_tank") ||
        sourceFeature.hasTag("man_made", "chimney") ||
        sourceFeature.hasTag("man_made", "stele")
    ) {
      return false;
    }

    String buildingValue = (String) (isPart ? sourceFeature.getTag("building:part") : sourceFeature.getTag("building"));

    return !buildingsWithoutWindows.contains(buildingValue);
  }

  private enum RoadwayExtensionSide {
    LEFT,
    RIGHT,
    BOTH
  }

  private static RoadwayExtensionSide getSidewalkSide(SourceFeature sourceFeature) {
    String sidewalkValue = (String)sourceFeature.getTag("sidewalk");
    String sidewalkBothValue = (String)sourceFeature.getTag("sidewalk:both");
    String sidewalkLeftValue = (String)sourceFeature.getTag("sidewalk:left");
    String sidewalkRightValue = (String)sourceFeature.getTag("sidewalk:right");

    sidewalkValue = sidewalkValue == null ? "" : sidewalkValue;
    sidewalkBothValue = sidewalkBothValue == null ? "" : sidewalkBothValue;
    sidewalkLeftValue = sidewalkLeftValue == null ? "" : sidewalkLeftValue;
    sidewalkRightValue = sidewalkRightValue == null ? "" : sidewalkRightValue;

    boolean isBoth = sidewalkBothValue.equals("yes") || sidewalkValue.equals("both");
    boolean isLeft = isBoth || sidewalkLeftValue.equals("yes") || sidewalkValue.equals("left");
    boolean isRight = isBoth || sidewalkRightValue.equals("yes") || sidewalkValue.equals("right");

    if (isLeft && isRight) {
      return RoadwayExtensionSide.BOTH;
    }

    if (isLeft) {
      return RoadwayExtensionSide.LEFT;
    }

    if (isRight) {
      return RoadwayExtensionSide.RIGHT;
    }

    return null;
  }

  private static RoadwayExtensionSide getCyclewaySide(SourceFeature sourceFeature) {
    String cyclewayBothValue = (String)sourceFeature.getTag("cycleway:both");
    String cyclewayLeftValue = (String)sourceFeature.getTag("cycleway:left");
    String cyclewayRightValue = (String)sourceFeature.getTag("cycleway:right");

    cyclewayBothValue = cyclewayBothValue == null ? "" : cyclewayBothValue;
    cyclewayLeftValue = cyclewayLeftValue == null ? "" : cyclewayLeftValue;
    cyclewayRightValue = cyclewayRightValue == null ? "" : cyclewayRightValue;

    boolean isBoth = cyclewayBothValue.equals("lane");
    boolean isLeft = isBoth || cyclewayLeftValue.equals("lane");
    boolean isRight = isBoth || cyclewayRightValue.equals("lane");

    if (isLeft && isRight) {
      return RoadwayExtensionSide.BOTH;
    }

    if (isLeft) {
      return RoadwayExtensionSide.LEFT;
    }

    if (isRight) {
      return RoadwayExtensionSide.RIGHT;
    }

    return null;
  }

  @Override
  public void processFeature(SourceFeature sourceFeature, FeatureCollector features) {
    if (isUnderground(sourceFeature)) {
      return;
    }

    if (sourceFeature.canBeLine()) {
      if (sourceFeature.hasTag("highway")) {
        var lanes = StreetsUtils.getRoadwayLanes(sourceFeature);

        var feature = features.line("highways")
          .setAttr("type", sourceFeature.getTag("highway"))
          .setAttr("surface", sourceFeature.getTag("surface"))
          .setAttr("width", StreetsUtils.getWidth(sourceFeature))
          .setAttr("laneMarkings", sourceFeature.getTag("lane_markings"))
          .setAttr("sidewalkSide", getSidewalkSide(sourceFeature))
          .setAttr("cyclewaySide", getCyclewaySide(sourceFeature))
          .setAttr("isOneway", StreetsUtils.isRoadwayOneway(sourceFeature))
          .setAttr("lanesForward", lanes.forward)
          .setAttr("lanesBackward", lanes.backward);

        setCommonFeatureParams(feature, sourceFeature);
        return;
      }

      if (sourceFeature.hasTag("aeroway", "runway") || sourceFeature.hasTag("aeroway", "taxiway")) {
        var feature = features.line("highways")
          .setAttr("type", sourceFeature.getTag("aeroway"))
          .setAttr("width", StreetsUtils.getWidth(sourceFeature));

        setCommonFeatureParams(feature, sourceFeature);
        return;
      }

      if (StreetsUtils.isRailway(sourceFeature)) {
        var feature = features.line("highways")
          .setAttr("type", sourceFeature.getTag("railway"))
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
          .setAttr("type", "hedge")
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
        var feature = features.line("powerLines");

        setCommonFeatureParams(feature, sourceFeature);
        return;
      }

      if (sourceFeature.hasTag("natural", "tree_row")) {
        var feature = features.line("natural")
          .setAttr("type", "tree_row")
          .setAttr("leafType", sourceFeature.getTag("leaf_type"))
          .setAttr("genus", sourceFeature.getTag("genus"))
          .setAttr("height", StreetsUtils.getHeight(sourceFeature))
          .setAttr("minHeight", StreetsUtils.getMinHeight(sourceFeature));

        setCommonFeatureParams(feature, sourceFeature);
        return;
      }

      if (sourceFeature.hasTag("waterway")) {
        var feature = features.line("water")
          .setAttr("type", sourceFeature.getTag("waterway"));

        setCommonFeatureParams(feature, sourceFeature);
        return;
      }
    }

    if (sourceFeature.canBePolygon()) {
      if (StreetsUtils.isWater(sourceFeature)) {
        var feature = features.polygon("water");

        setWaterFeatureParams(feature, sourceFeature);
        return;
      }

      if (
        sourceFeature.hasTag("building:part") || (
          sourceFeature.hasTag("building") && !this.isBuildingOutline(sourceFeature)
        )
      ) {
        Boolean isPart = sourceFeature.hasTag("building:part");

        var feature = features.polygon("buildings")
          .setAttr("type", sourceFeature.getTag("building"))
          .setAttr("height", StreetsUtils.getHeight(sourceFeature))
          .setAttr("minHeight", StreetsUtils.getMinHeight(sourceFeature))
          .setAttr("roofHeight", sourceFeature.getTag("roof:height"))
          .setAttr("buildingLevels", sourceFeature.getTag("building:levels"))
          .setAttr("roofLevels", sourceFeature.getTag("roof:levels"))
          .setAttr("roofMaterial", sourceFeature.getTag("roof:material"))
          .setAttr("roofType", sourceFeature.getTag("roof:shape"))
          .setAttr("roofOrientation", sourceFeature.getTag("roof:orientation"))
          .setAttr("roofDirection", StreetsUtils.getDirection(sourceFeature))
          .setAttr("roofAngle", StreetsUtils.getAngle(sourceFeature))
          .setAttr("roofColor", sourceFeature.getTag("roof:colour"))
          .setAttr("color", sourceFeature.getTag("building:colour"))
          .setAttr("hasWindows", isBuildingHasWindows(sourceFeature, isPart))
          .setBufferPixels(256);

        setCommonFeatureParams(feature, sourceFeature);
      }

      if (
        sourceFeature.hasTag("area:highway") ||
          (sourceFeature.hasTag("highway") && sourceFeature.hasTag("area", "yes"))
      ) {
        var feature = features.polygon("highways")
          .setAttr("type", sourceFeature.getTag("highway"));

        setCommonFeatureParams(feature, sourceFeature);
        return;
      }

      if (sourceFeature.hasTag("landuse", "brownfield", "construction", "grass", "farmland")) {
        var feature = features.polygon("landuse")
          .setAttr("type", sourceFeature.getTag("landuse"));

        setCommonFeatureParams(feature, sourceFeature);
        return;
      }

      if (sourceFeature.hasTag("natural", "wood", "forest", "scrub", "sand", "beach", "rock", "bare_rock")) {
        var feature = features.polygon("natural")
          .setAttr("type", sourceFeature.getTag("natural"));

        setCommonFeatureParams(feature, sourceFeature);
        return;
      }

      if (sourceFeature.hasTag("leisure", "garden")) {
        var feature = features.polygon("natural")
          .setAttr("type", "garden");

        setCommonFeatureParams(feature, sourceFeature);
        return;
      }

      if (sourceFeature.hasTag("leisure", "fairway")) {
        var feature = features.polygon("natural")
          .setAttr("type", "manicuredGrass");

        setCommonFeatureParams(feature, sourceFeature);
        return;
      }
    }

    if (sourceFeature.isPoint()) {
      if (sourceFeature.hasTag("natural", "tree")) {
        var feature = features.point("point")
          .setAttr("type", "tree")
          .setAttr("leaf_type", sourceFeature.getTag("leaf_type"))
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
      .setZoomRange(8, 16)
      .setPixelToleranceAtAllZooms(0)
      .setMinPixelSize(0);
  }

  private static void setWaterFeatureParams(FeatureCollector.Feature feature, SourceFeature sourceFeature) {
    feature
      .setZoomRange(8, 16)
      .setPixelToleranceAtAllZooms(0)
      .setMinPixelSize(0);
  }

  public static void main(String[] args) throws Exception {
    run(Arguments.fromArgsOrConfigFile(args));
  }

  public static void run(Arguments args) throws Exception {
    Planetiler.create(args)
      .setProfile(new MyProfile())
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

    return items.size() > 1 ? FeatureMerge.mergeOverlappingPolygons(items, 0) : items;
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
