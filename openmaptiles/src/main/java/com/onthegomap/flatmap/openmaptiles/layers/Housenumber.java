package com.onthegomap.flatmap.openmaptiles.layers;

import com.onthegomap.flatmap.Arguments;
import com.onthegomap.flatmap.FeatureCollector;
import com.onthegomap.flatmap.Translations;
import com.onthegomap.flatmap.monitoring.Stats;
import com.onthegomap.flatmap.openmaptiles.generated.OpenMapTilesSchema;
import com.onthegomap.flatmap.openmaptiles.generated.Tables;

public class Housenumber implements OpenMapTilesSchema.Housenumber, Tables.OsmHousenumberPoint.Handler {

  public Housenumber(Translations translations, Arguments args, Stats stats) {
  }

  @Override
  public void process(Tables.OsmHousenumberPoint element, FeatureCollector features) {
    features.centroidIfConvex(LAYER_NAME)
      .setBufferPixels(BUFFER_SIZE)
      .setAttr(Fields.HOUSENUMBER, element.housenumber())
      .setZoomRange(14, 14);
  }
}
