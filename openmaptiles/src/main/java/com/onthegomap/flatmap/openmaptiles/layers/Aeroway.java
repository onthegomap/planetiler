package com.onthegomap.flatmap.openmaptiles.layers;

import com.onthegomap.flatmap.Arguments;
import com.onthegomap.flatmap.FeatureCollector;
import com.onthegomap.flatmap.Translations;
import com.onthegomap.flatmap.monitoring.Stats;
import com.onthegomap.flatmap.openmaptiles.generated.OpenMapTilesSchema;
import com.onthegomap.flatmap.openmaptiles.generated.Tables;

public class Aeroway implements OpenMapTilesSchema.Aeroway, Tables.OsmAerowayLinestring.Handler,
  Tables.OsmAerowayPolygon.Handler,
  Tables.OsmAerowayPoint.Handler {

  public Aeroway(Translations translations, Arguments args, Stats stats) {
  }

  @Override
  public void process(Tables.OsmAerowayPolygon element, FeatureCollector features) {
    features.polygon(LAYER_NAME)
      .setZoomRange(10, 14)
      .setMinPixelSize(2)
      .setAttr(Fields.CLASS, element.aeroway())
      .setAttr(Fields.REF, element.ref());
  }

  @Override
  public void process(Tables.OsmAerowayLinestring element, FeatureCollector features) {
    features.line(LAYER_NAME)
      .setZoomRange(10, 14)
      .setAttr(Fields.CLASS, element.aeroway())
      .setAttr(Fields.REF, element.ref());
  }

  @Override
  public void process(Tables.OsmAerowayPoint element, FeatureCollector features) {
    features.point(LAYER_NAME)
      .setZoomRange(14, 14)
      .setAttr(Fields.CLASS, element.aeroway())
      .setAttr(Fields.REF, element.ref());
  }
}
