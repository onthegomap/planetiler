package com.onthegomap.flatmap.openmaptiles.layers;

import com.onthegomap.flatmap.Arguments;
import com.onthegomap.flatmap.FeatureCollector;
import com.onthegomap.flatmap.SourceFeature;
import com.onthegomap.flatmap.Translations;
import com.onthegomap.flatmap.monitoring.Stats;
import com.onthegomap.flatmap.openmaptiles.MultiExpression;
import com.onthegomap.flatmap.openmaptiles.OpenMapTilesProfile;
import com.onthegomap.flatmap.openmaptiles.Utils;
import com.onthegomap.flatmap.openmaptiles.generated.OpenMapTilesSchema;
import com.onthegomap.flatmap.openmaptiles.generated.Tables;

public class Water implements OpenMapTilesSchema.Water, Tables.OsmWaterPolygon.Handler,
  OpenMapTilesProfile.NaturalEarthProcessor, OpenMapTilesProfile.OsmWaterPolygonProcessor {

  private final MultiExpression.MultiExpressionIndex<String> classMapping;

  public Water(Translations translations, Arguments args, Stats stats) {
    this.classMapping = FieldMappings.Class.index();
  }

  @Override
  public void process(Tables.OsmWaterPolygon element, FeatureCollector features) {
    if (!"bay".equals(element.natural())) {
      features.polygon(LAYER_NAME)
        .setBufferPixels(BUFFER_SIZE)
        .setMinPixelSizeBelowZoom(11, 2)
        .setZoomRange(6, 14)
        .setAttr(Fields.INTERMITTENT, element.isIntermittent() ? 1 : 0)
        .setAttrWithMinzoom(Fields.BRUNNEL, Utils.brunnel(element.isBridge(), element.isTunnel()), 12)
        .setAttr(Fields.CLASS, classMapping.getOrElse(element.source().properties(), FieldValues.CLASS_RIVER));
    }
  }

  @Override
  public void processNaturalEarth(String table, SourceFeature feature, FeatureCollector features) {
    record WaterInfo(int minZoom, int maxZoom, String clazz) {}
    WaterInfo info = switch (table) {
      case "ne_10m_ocean" -> new WaterInfo(5, 5, FieldValues.CLASS_OCEAN);
      case "ne_50m_ocean" -> new WaterInfo(2, 4, FieldValues.CLASS_OCEAN);
      case "ne_110m_ocean" -> new WaterInfo(0, 1, FieldValues.CLASS_OCEAN);

      case "ne_10m_lakes" -> new WaterInfo(4, 5, FieldValues.CLASS_LAKE);
      case "ne_50m_lakes" -> new WaterInfo(2, 3, FieldValues.CLASS_LAKE);
      case "ne_110m_lakes" -> new WaterInfo(0, 1, FieldValues.CLASS_LAKE);
      default -> null;
    };
    if (info != null) {
      features.polygon(LAYER_NAME)
        .setBufferPixels(BUFFER_SIZE)
        .setZoomRange(info.minZoom, info.maxZoom)
        .setAttr(Fields.CLASS, info.clazz);
    }
  }

  @Override
  public void processOsmWater(SourceFeature feature, FeatureCollector features) {
    features.polygon(LAYER_NAME)
      .setBufferPixels(BUFFER_SIZE)
      .setAttr(Fields.CLASS, FieldValues.CLASS_OCEAN)
      .setZoomRange(6, 14);
  }
}
