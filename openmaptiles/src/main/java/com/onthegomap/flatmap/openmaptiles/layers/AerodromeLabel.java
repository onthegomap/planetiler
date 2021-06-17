package com.onthegomap.flatmap.openmaptiles.layers;

import static com.onthegomap.flatmap.openmaptiles.Utils.nullIfEmpty;

import com.onthegomap.flatmap.Arguments;
import com.onthegomap.flatmap.FeatureCollector;
import com.onthegomap.flatmap.Translations;
import com.onthegomap.flatmap.monitoring.Stats;
import com.onthegomap.flatmap.openmaptiles.LanguageUtils;
import com.onthegomap.flatmap.openmaptiles.MultiExpression;
import com.onthegomap.flatmap.openmaptiles.Utils;
import com.onthegomap.flatmap.openmaptiles.generated.Layers;
import com.onthegomap.flatmap.openmaptiles.generated.Tables;

public class AerodromeLabel implements Layers.AerodromeLabel, Tables.OsmAerodromeLabelPoint.Handler {

  private final MultiExpression.MultiExpressionIndex<String> classLookup;
  private final Translations translations;

  public AerodromeLabel(Translations translations, Arguments args, Stats stats) {
    this.classLookup = FieldMappings.Class.index();
    this.translations = translations;
  }

  @Override
  public void process(Tables.OsmAerodromeLabelPoint element, FeatureCollector features) {
    features.centroid(LAYER_NAME)
      .setBufferPixels(BUFFER_SIZE)
      .setZoomRange(10, 14)
      .setAttrs(LanguageUtils.getNames(element.source().properties(), translations))
      .setAttrs(Utils.elevationTags(element.ele()))
      .setAttr(Fields.IATA, nullIfEmpty(element.iata()))
      .setAttr(Fields.ICAO, nullIfEmpty(element.icao()))
      .setAttr(Fields.CLASS, classLookup.getOrElse(element.source().properties(), "other"));
  }
}
