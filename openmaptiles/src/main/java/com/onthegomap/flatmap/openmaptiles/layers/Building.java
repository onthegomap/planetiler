package com.onthegomap.flatmap.openmaptiles.layers;

import static com.onthegomap.flatmap.Parse.parseDoubleOrNull;
import static com.onthegomap.flatmap.openmaptiles.Utils.coalesce;
import static java.util.Map.entry;

import com.graphhopper.reader.ReaderRelation;
import com.onthegomap.flatmap.Arguments;
import com.onthegomap.flatmap.FeatureCollector;
import com.onthegomap.flatmap.FeatureMerge;
import com.onthegomap.flatmap.Translations;
import com.onthegomap.flatmap.VectorTileEncoder;
import com.onthegomap.flatmap.geo.GeometryException;
import com.onthegomap.flatmap.monitoring.Stats;
import com.onthegomap.flatmap.openmaptiles.OpenMapTilesProfile;
import com.onthegomap.flatmap.openmaptiles.generated.OpenMapTilesSchema;
import com.onthegomap.flatmap.openmaptiles.generated.Tables;
import com.onthegomap.flatmap.read.OpenStreetMapReader;
import java.util.List;
import java.util.Locale;
import java.util.Map;

public class Building implements OpenMapTilesSchema.Building,
  Tables.OsmBuildingPolygon.Handler,
  OpenMapTilesProfile.FeaturePostProcessor,
  OpenMapTilesProfile.OsmRelationPreprocessor {

  private final boolean mergeZ13Buildings;

  private static final Map<String, String> MATERIAL_COLORS = Map.ofEntries(
    entry("cement_block", "#6a7880"),
    entry("brick", "#bd8161"),
    entry("plaster", "#dadbdb"),
    entry("wood", "#d48741"),
    entry("concrete", "#d3c2b0"),
    entry("metal", "#b7b1a6"),
    entry("stone", "#b4a995"),
    entry("mud", "#9d8b75"),
    entry("steel", "#b7b1a6"), // same as metal
    entry("glass", "#5a81a0"),
    entry("traditional", "#bd8161"), // same as brick
    entry("masonry", "#bd8161"), // same as brick
    entry("Brick", "#bd8161"), // same as brick
    entry("tin", "#b7b1a6"), // same as metal
    entry("timber_framing", "#b3b0a9"),
    entry("sandstone", "#b4a995"), // same as stone
    entry("clay", "#9d8b75") // same as mud
  );

  public Building(Translations translations, Arguments args, Stats stats) {
    this.mergeZ13Buildings = args.get(
      "building_merge_z13",
      "building layer: merge nearby buildings at z13",
      true
    );
  }

  private static record BuildingRelationInfo(long id) implements OpenStreetMapReader.RelationInfo {

    @Override
    public long estimateMemoryUsageBytes() {
      return 29;
    }
  }

  @Override
  public List<OpenStreetMapReader.RelationInfo> preprocessOsmRelation(ReaderRelation relation) {
    if (relation.hasTag("type", "building")) {
      return List.of(new BuildingRelationInfo(relation.getId()));
    }
    return null;
  }

  @Override
  public void process(Tables.OsmBuildingPolygon element, FeatureCollector features) {
    Boolean hide3d = null;
    var relations = element.source().relationInfo(BuildingRelationInfo.class);
    for (var relation : relations) {
      if ("outline".equals(relation.role())) {
        hide3d = true;
        break;
      }
    }

    String color = element.colour();
    if (color == null && element.material() != null) {
      color = MATERIAL_COLORS.get(element.material());
    }
    if (color != null) {
      color = color.toLowerCase(Locale.ROOT);
    }

    Double height = coalesce(
      parseDoubleOrNull(element.height()),
      parseDoubleOrNull(element.buildingheight())
    );
    Double minHeight = coalesce(
      parseDoubleOrNull(element.minHeight()),
      parseDoubleOrNull(element.buildingminHeight())
    );
    Double levels = coalesce(
      parseDoubleOrNull(element.levels()),
      parseDoubleOrNull(element.buildinglevels())
    );
    Double minLevels = coalesce(
      parseDoubleOrNull(element.minLevel()),
      parseDoubleOrNull(element.buildingminLevel())
    );

    int renderHeight = (int) Math.ceil(height != null ? height
      : levels != null ? (levels * 3.66) : 5);
    int renderMinHeight = (int) Math.floor(minHeight != null ? minHeight
      : minLevels != null ? (minLevels * 3.66) : 0);

    if (renderHeight < 3660 && renderMinHeight < 3660) {
      var feature = features.polygon(LAYER_NAME).setBufferPixels(BUFFER_SIZE)
        .setZoomRange(13, 14)
        .setMinPixelSize(2)
        .setAttrWithMinzoom(Fields.RENDER_HEIGHT, renderHeight, 14)
        .setAttrWithMinzoom(Fields.RENDER_MIN_HEIGHT, renderMinHeight, 14)
        .setAttrWithMinzoom(Fields.COLOUR, color, 14)
        .setAttrWithMinzoom(Fields.HIDE_3D, hide3d, 14);
      if (mergeZ13Buildings) {
        feature
          .setMinPixelSize(0.25)
          .setPixelTolerance(0.25); // improves performance of the building merge ~50% over default
      }
    }
  }

  @Override
  public List<VectorTileEncoder.Feature> postProcess(int zoom,
    List<VectorTileEncoder.Feature> items) throws GeometryException {
    return (mergeZ13Buildings && zoom == 13) ? FeatureMerge.mergePolygons(items, 4, 4, 0.5, 0.5) : items;
  }
}
