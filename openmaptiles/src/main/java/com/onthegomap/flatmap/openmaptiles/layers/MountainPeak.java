package com.onthegomap.flatmap.openmaptiles.layers;

import static com.onthegomap.flatmap.openmaptiles.Utils.coalesce;
import static com.onthegomap.flatmap.openmaptiles.Utils.metersToFeet;
import static com.onthegomap.flatmap.openmaptiles.Utils.nullIf;

import com.onthegomap.flatmap.FeatureCollector;
import com.onthegomap.flatmap.Parse;
import com.onthegomap.flatmap.openmaptiles.generated.Layers;
import com.onthegomap.flatmap.openmaptiles.generated.Tables;

public class MountainPeak implements
  Layers.MountainPeak,
  Tables.OsmPeakPoint.Handler {

  @Override
  public void process(Tables.OsmPeakPoint element, FeatureCollector features) {
    Integer meters = Parse.parseIntSubstring(element.ele());
    if (meters != null) {
      features.point(LAYER_NAME)
        .setAttr(Fields.NAME, element.name())
        .setAttr(Fields.NAME_EN, coalesce(nullIf(element.nameEn(), ""), element.name()))
        .setAttr(Fields.NAME_DE, coalesce(nullIf(element.nameDe(), ""), element.name()))
        .setAttr(Fields.CLASS, element.source().getTag("natural"))
        .setAttr(Fields.ELE, meters)
        .setAttr(Fields.ELE_FT, metersToFeet(meters))
        .setBufferPixels(BUFFER_SIZE)
        .setZoomRange(7, 14)
        .setLabelGridSizeAndLimit(13, 100, 5);
    }
  }
}
