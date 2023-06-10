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
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.algorithm.MinimumAreaRectangle;
import org.locationtech.jts.algorithm.construct.MaximumInscribedCircle;
import org.locationtech.jts.geom.LineString;

public class StreetsProfile implements Profile {
  @Override
  public String name() {
    return "Streets GL Profile";
  }

  private static void processPoint(SourceFeature sourceFeature, FeatureCollector features) {
    if (sourceFeature.hasTag("natural", "tree")) {
      var feature = features.point("point")
        .setAttr("type", "tree")
        .setAttr("leafType", StreetsUtils.getLeafType(sourceFeature))
        .setAttr("genus", StreetsUtils.getGenus(sourceFeature))
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
        .setAttr("type", "memorial")
        .setAttr("height", StreetsUtils.getHeight(sourceFeature))
        .setAttr("minHeight", StreetsUtils.getMinHeight(sourceFeature))
        .setAttr("direction", StreetsUtils.getDirection(sourceFeature));

      setCommonFeatureParams(feature, sourceFeature);
      return;
    }

    if (StreetsUtils.isStatue(sourceFeature)) {
      var feature = features.point("point")
        .setAttr("type", "statue")
        .setAttr("height", StreetsUtils.getHeight(sourceFeature))
        .setAttr("minHeight", StreetsUtils.getMinHeight(sourceFeature))
        .setAttr("direction", StreetsUtils.getDirection(sourceFeature));

      setCommonFeatureParams(feature, sourceFeature);
      return;
    }

    if (StreetsUtils.isSculpture(sourceFeature)) {
      var feature = features.point("point")
        .setAttr("type", "sculpture")
        .setAttr("height", StreetsUtils.getHeight(sourceFeature))
        .setAttr("minHeight", StreetsUtils.getMinHeight(sourceFeature))
        .setAttr("direction", StreetsUtils.getDirection(sourceFeature));

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
      var feature = features.point("point")
        .setAttr("type", "roundabout")
        .setAttr("surface", StreetsUtils.getSurface(sourceFeature));

      setCommonFeatureParams(feature, sourceFeature);
      return;
    }

    if (sourceFeature.hasTag("highway", "bus_stop")) {
      var feature = features.point("point")
        .setAttr("type", "busStop")
        .setAttr("minHeight", StreetsUtils.getMinHeight(sourceFeature));

      setCommonFeatureParams(feature, sourceFeature);
      return;
    }

    if (sourceFeature.hasTag("aeroway", "helipad")) {
      var feature = features.point("point")
        .setAttr("type", "helipad")
        .setAttr("surface", StreetsUtils.getSurface(sourceFeature));

      setCommonFeatureParams(feature, sourceFeature);
      return;
    }

    if (sourceFeature.hasTag("natural", "rock")) {
      var feature = features.point("point").setAttr("type", "rock")
        .setAttr("height", StreetsUtils.getHeight(sourceFeature))
        .setAttr("minHeight", StreetsUtils.getMinHeight(sourceFeature));

      setCommonFeatureParams(feature, sourceFeature);
      return;
    }

    if (sourceFeature.hasTag("power", "pole")) {
      var feature = features.point("point")
        .setAttr("type", "utilityPole")
        .setAttr("minHeight", StreetsUtils.getMinHeight(sourceFeature));

      setCommonFeatureParams(feature, sourceFeature);
      return;
    }

    if (sourceFeature.hasTag("power", "tower")) {
      var feature = features.point("point")
        .setAttr("type", "transmissionTower")
        .setAttr("minHeight", StreetsUtils.getMinHeight(sourceFeature));

      setCommonFeatureParams(feature, sourceFeature);
      return;
    }

    if (sourceFeature.hasTag("amenity", "fountain")) {
      var feature = features.point("point")
        .setAttr("type", "fountain");

      setCommonFeatureParams(feature, sourceFeature);
      return;
    }

    if (sourceFeature.hasTag("man_made", "flagpole")) {
      var feature = features.point("point")
        .setAttr("type", "flagpole")
        .setAttr("height", StreetsUtils.getHeight(sourceFeature))
        .setAttr("minHeight", StreetsUtils.getMinHeight(sourceFeature))
        .setAttr("wikidata", StreetsUtils.getFlagWikidata(sourceFeature))
        .setAttr("country", StreetsUtils.getFlagCountry(sourceFeature));

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
        .setAttr("surface", StreetsUtils.getSurface(sourceFeature))
        .setAttr("width", StreetsUtils.getWidth(sourceFeature))
        .setAttr("laneMarkings", StreetsUtils.getLaneMarkings(sourceFeature))
        .setAttr("sidewalkSide", StreetsUtils.convertRoadwayExtensionSideToInteger(StreetsUtils.getSidewalkSide(sourceFeature)))
        .setAttr("cyclewaySide", StreetsUtils.convertRoadwayExtensionSideToInteger(StreetsUtils.getCyclewaySide(sourceFeature)))
        .setAttr("oneway", StreetsUtils.isRoadwayOneway(sourceFeature))
        .setAttr("lanes", lanes.both)
        .setAttr("lanesForward", lanes.forward)
        .setAttr("lanesBackward", lanes.backward);

      setCommonFeatureParams(feature, sourceFeature);
      feature.setBufferPixels(24);
      return;
    }

    if (sourceFeature.hasTag("aeroway", "runway", "taxiway")) {
      var feature = features.line("highways")
        .setAttr("type", "path")
        .setAttr("pathType", sourceFeature.getTag("aeroway"))
        .setAttr("surface", StreetsUtils.getSurface(sourceFeature))
        .setAttr("width", StreetsUtils.getWidth(sourceFeature));

      setCommonFeatureParams(feature, sourceFeature);
      feature.setBufferPixels(24);
      return;
    }

    if (StreetsUtils.isRailway(sourceFeature)) {
      var feature = features.line("highways")
        .setAttr("type", "railway")
        .setAttr("railwayType", StreetsUtils.getRailwayType(sourceFeature))
        .setAttr("gauge", StreetsUtils.getGauge(sourceFeature));

      setCommonFeatureParams(feature, sourceFeature);
      return;
    }

    if (sourceFeature.hasTag("barrier", "fence")) {
      var feature = features.line("barriers")
        .setAttr("type", "fence")
        .setAttr("fenceType", StreetsUtils.getFenceType(sourceFeature))
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
        .setAttr("wallType", StreetsUtils.getWallType(sourceFeature))
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
        .setAttr("leafType", StreetsUtils.getLeafType(sourceFeature))
        .setAttr("genus", StreetsUtils.getGenus(sourceFeature))
        .setAttr("height", StreetsUtils.getHeight(sourceFeature))
        .setAttr("minHeight", StreetsUtils.getMinHeight(sourceFeature));

      setCommonFeatureParams(feature, sourceFeature);
      return;
    }

    if (sourceFeature.hasTag("waterway")) {
      var feature = features.line("water")
        .setAttr("type", "waterway")
        .setAttr("waterwayType", StreetsUtils.getWaterwayType(sourceFeature));

      setCommonFeatureParams(feature, sourceFeature);
    }

    if (sourceFeature.hasTag("leisure", "track")) {
      var feature = features.line("common")
        .setAttr("type", "track")
        .setAttr("sport", sourceFeature.getTag("sport"))
        .setAttr("surface", StreetsUtils.getSurface(sourceFeature));

      setCommonFeatureParams(feature, sourceFeature);
    }
  }

  private void setPolygonOMBB(FeatureCollector.Feature feature) {
    Geometry geometry = feature.getGeometry();
    Geometry ombb = MinimumAreaRectangle.getMinimumRectangle(geometry);

    var coords = ombb.getCoordinates();

    feature.setAttr("@ombb00", coords[0].x);
    feature.setAttr("@ombb01", coords[0].y);

    feature.setAttr("@ombb10", coords[1].x);
    feature.setAttr("@ombb11", coords[1].y);

    feature.setAttr("@ombb20", coords[2].x);
    feature.setAttr("@ombb21", coords[2].y);

    feature.setAttr("@ombb30", coords[3].x);
    feature.setAttr("@ombb31", coords[3].y);
  }

  private void setPolygonPoleOfInaccessibility(FeatureCollector.Feature feature) {
    Geometry geometry = feature.getGeometry();
    LineString poi = MaximumInscribedCircle.getRadiusLine(geometry, 1e-9);

    var coords = poi.getCoordinates();

    feature.setAttr("@poiX", coords[0].x);
    feature.setAttr("@poiY", coords[0].y);
    feature.setAttr("@poiR", poi.getLength());
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
      String buildingType = isPart ? (String) sourceFeature.getTag("building:part") : (String) sourceFeature.getTag("building");

      var feature = features.polygon("buildings")
        .setAttr("type", "building")
        .setAttr("isPart", isPart)
        .setAttr("buildingType", buildingType)
        .setAttr("name", sourceFeature.getTag("name"))
        .setAttr("height", StreetsUtils.getHeight(sourceFeature))
        .setAttr("minHeight", StreetsUtils.getMinHeight(sourceFeature))
        .setAttr("levels", StreetsUtils.getBuildingLevels(sourceFeature))
        .setAttr("material", StreetsUtils.getBuildingMaterial(sourceFeature))
        .setAttr("roofHeight", StreetsUtils.getRoofHeight(sourceFeature))
        .setAttr("roofLevels", StreetsUtils.getRoofLevels(sourceFeature))
        .setAttr("roofMaterial", StreetsUtils.getRoofMaterial(sourceFeature))
        .setAttr("roofMaterial", StreetsUtils.getRoofMaterial(sourceFeature))
        .setAttr("roofType", StreetsUtils.getRoofShape(sourceFeature))
        .setAttr("roofOrientation", StreetsUtils.getRoofOrientation(sourceFeature))
        .setAttr("roofDirection", StreetsUtils.getRoofDirection(sourceFeature))
        .setAttr("roofAngle", StreetsUtils.getAngle(sourceFeature))
        .setAttr("roofColor", StreetsUtils.getRoofColor(sourceFeature))
        .setAttr("color", StreetsUtils.getBuildingColor(sourceFeature))
        .setAttr("windows", StreetsUtils.getBuildingWindows(sourceFeature))
        .setAttr("defaultRoof", StreetsUtils.getBuildingDefaultRoof(sourceFeature));

      setPolygonOMBB(feature);
      setCommonFeatureParams(feature, sourceFeature);

      feature.setBufferPixels(isPart ? 512 : 256);
    }

    if (isArea(sourceFeature)) {
      if (sourceFeature.hasTag("highway")) {
        var feature = features.polygon("highways")
          .setAttr("type", "path")
          .setAttr("pathType", sourceFeature.getTag("highway"));

        setCommonFeatureParams(feature, sourceFeature);
        return true;
      }

      if (sourceFeature.hasTag("man_made", "pier")) {
        var feature = features.polygon("common")
          .setAttr("type", "pier")
          .setAttr("surface", StreetsUtils.getSurface(sourceFeature));

        setCommonFeatureParams(feature, sourceFeature);
        return true;
      }
    }

    if (sourceFeature.hasTag("area:highway")) {
      var feature = features.polygon("highways")
        .setAttr("type", "path")
        .setAttr("pathType", sourceFeature.getTag("area:highway"));

      setCommonFeatureParams(feature, sourceFeature);
      return true;
    }

    if (sourceFeature.hasTag("landuse", "brownfield")) {
      var feature = features.polygon("common")
        .setAttr("type", "brownfield");

      setCommonFeatureParams(feature, sourceFeature);
      setPolygonPoleOfInaccessibility(feature);
      return true;
    }

    if (sourceFeature.hasTag("landuse", "construction")) {
      var feature = features.polygon("common")
        .setAttr("type", "construction");

      setCommonFeatureParams(feature, sourceFeature);
      setPolygonPoleOfInaccessibility(feature);
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
        .setAttr("type", "farmland")
        .setAttr("crop", StreetsUtils.getCrop(sourceFeature));

      setPolygonOMBB(feature);
      setCommonFeatureParams(feature, sourceFeature);
      return true;
    }

    if (sourceFeature.hasTag("natural", "scrub")) {
      var feature = features.polygon("natural")
        .setAttr("type", "scrub");

      setCommonFeatureParams(feature, sourceFeature);
      return true;
    }

    if (
      sourceFeature.hasTag("natural", "wood") ||
        sourceFeature.hasTag("landuse", "forest") ||
        sourceFeature.hasTag("landcover", "trees")
    ) {
      var feature = features.polygon("natural")
        .setAttr("type", "forest")
        .setAttr("leafType", StreetsUtils.getLeafType(sourceFeature))
        .setAttr("genus", StreetsUtils.getGenus(sourceFeature));

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
        .setAttr("sport", sourceFeature.getTag("sport"))
        .setAttr("hoops", StreetsUtils.parseUnsignedInt((String) sourceFeature.getTag("hoops")))
        .setAttr("surface", StreetsUtils.getSurface(sourceFeature));

      setPolygonOMBB(feature);
      setCommonFeatureParams(feature, sourceFeature);
      return true;
    }

    if (sourceFeature.hasTag("leisure", "track")) {
      var feature = features.polygon("common")
        .setAttr("type", "track")
        .setAttr("sport", sourceFeature.getTag("sport"))
        .setAttr("surface", StreetsUtils.getSurface(sourceFeature));

      setPolygonOMBB(feature);
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
        .setAttr("surface", StreetsUtils.getSurface(sourceFeature));

      setCommonFeatureParams(feature, sourceFeature);
      return true;
    }

    if (sourceFeature.hasTag("man_made", "bridge")) {
      var feature = features.polygon("common")
        .setAttr("type", "bridge");

      setCommonFeatureParams(feature, sourceFeature);
      return true;
    }

    if (sourceFeature.hasTag("aeroway", "helipad")) {
      var feature = features.polygon("common")
        .setAttr("type", "helipad")
        .setAttr("surface", StreetsUtils.getSurface(sourceFeature));

      setPolygonOMBB(feature);
      setCommonFeatureParams(feature, sourceFeature);
      return true;
    }

    if (sourceFeature.hasTag("aeroway", "apron")) {
      var feature = features.polygon("common")
        .setAttr("type", "apron")
        .setAttr("surface", StreetsUtils.getSurface(sourceFeature));

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
      .setMinPixelSize(0)
      .setBufferPixels(4);
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
    if (layer.equals("water") && items.size() > 1) {
      return FeatureMerge.mergeOverlappingPolygons(items, 0);
    }

    if (layer.equals("buildings")) {
      boolean hasParts = false;

      for (VectorTile.Feature item : items) {
        if ((boolean) item.attrs().get("isPart")) {
          hasParts = true;
          break;
        }
      }

      if (hasParts) {
        var parts = new ArrayList<BuildingPartWithEnvelope>();
        var outlines = new ArrayList<BuildingOutlineWithEnvelope>();

        for (VectorTile.Feature item : items) {
          Geometry geometry = item.geometry().decode();
          Envelope bbox = geometry.getEnvelopeInternal();

          boolean isPart = (boolean) item.attrs().get("isPart");

          if (isPart) {
            parts.add(new BuildingPartWithEnvelope(item, geometry, bbox));
          } else {
            outlines.add(new BuildingOutlineWithEnvelope(item, geometry, bbox));
          }
        }

        for (BuildingOutlineWithEnvelope outline : outlines) {
          for (BuildingPartWithEnvelope part : parts) {
            if (!outline.envelope.intersects(part.envelope)) {
              continue;
            }

            outline.geometryCopy = outline.geometryCopy.difference(part.geometry);
          }

          var initialOutlineArea = outline.geometry.getArea();
          var newOutlineArea = outline.geometryCopy.getArea();

          if (newOutlineArea / initialOutlineArea < 0.1) {
            items.remove(outline.feature);
          }
        }

        for (BuildingPartWithEnvelope part : parts) {
          if (!part.envelope.intersects(BuildingPartWithEnvelope.TileBoundsEnvelope)) {
            items.remove(part.feature);
          }
        }
      }

      for (VectorTile.Feature item : items) {
        item.attrs().remove("isPart");
      }
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

  private boolean isArea(SourceFeature sourceFeature) {
    return sourceFeature.hasTag("area", "yes") || sourceFeature.hasTag("type", "multipolygon");
  }

  private static class BuildingPartWithEnvelope {
    static final Envelope TileBoundsEnvelope = new Envelope(-4, 260, -4, 260);

    VectorTile.Feature feature;
    Geometry geometry;
    Envelope envelope;

    public BuildingPartWithEnvelope(VectorTile.Feature feature, Geometry geometry, Envelope envelope) {
      this.feature = feature;
      this.geometry = geometry;
      this.envelope = envelope;
    }
  }

  private static class BuildingOutlineWithEnvelope {
    VectorTile.Feature feature;
    Geometry geometry;
    Geometry geometryCopy;
    Envelope envelope;

    public BuildingOutlineWithEnvelope(VectorTile.Feature feature, Geometry geometry, Envelope envelope) {
      this.feature = feature;
      this.geometry = geometry;
      this.envelope = envelope;
      this.geometryCopy = geometry.copy();
    }
  }
}
