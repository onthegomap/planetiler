package com.onthegomap.flatmap.openmaptiles.layers;

import static com.onthegomap.flatmap.openmaptiles.LanguageUtils.getNames;
import static com.onthegomap.flatmap.openmaptiles.Utils.elevationTags;
import static com.onthegomap.flatmap.openmaptiles.Utils.nullIf;

import com.onthegomap.flatmap.Arguments;
import com.onthegomap.flatmap.FeatureCollector;
import com.onthegomap.flatmap.Parse;
import com.onthegomap.flatmap.Translations;
import com.onthegomap.flatmap.VectorTileEncoder;
import com.onthegomap.flatmap.monitoring.Stats;
import com.onthegomap.flatmap.openmaptiles.OpenMapTilesProfile;
import com.onthegomap.flatmap.openmaptiles.generated.Layers;
import com.onthegomap.flatmap.openmaptiles.generated.Tables;
import java.util.List;

public class MountainPeak implements
  Layers.MountainPeak,
  Tables.OsmPeakPoint.Handler,
  OpenMapTilesProfile.FeaturePostProcessor {

  private final Translations translations;

  public MountainPeak(Translations translations, Arguments args, Stats stats) {
    this.translations = translations;
  }

  @Override
  public void process(Tables.OsmPeakPoint element, FeatureCollector features) {
    Integer meters = Parse.parseIntSubstring(element.ele());
    if (meters != null && Math.abs(meters) < 10_000) {
      features.point(LAYER_NAME)
        .setAttr(Fields.CLASS, element.source().getTag("natural"))
        .setAttrs(getNames(element.source().properties(), translations))
        .setAttrs(elevationTags(meters))
        .setBufferPixels(BUFFER_SIZE)
        .setZorder(
          meters +
            (nullIf(element.wikipedia(), "") == null ? 10_000 : 0) +
            (nullIf(element.name(), "") == null ? 10_000 : 0)
        )
        .setZoomRange(7, 14)
        .setLabelGridSizeAndLimit(13, 100, 5);
    }
  }

  @Override
  public void postProcess(int zoom, List<VectorTileEncoder.Feature> items) {

  }
}
