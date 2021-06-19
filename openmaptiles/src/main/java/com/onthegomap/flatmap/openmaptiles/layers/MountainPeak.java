package com.onthegomap.flatmap.openmaptiles.layers;

import static com.onthegomap.flatmap.openmaptiles.Utils.elevationTags;
import static com.onthegomap.flatmap.openmaptiles.Utils.nullIfEmpty;

import com.carrotsearch.hppc.LongIntHashMap;
import com.carrotsearch.hppc.LongIntMap;
import com.onthegomap.flatmap.Arguments;
import com.onthegomap.flatmap.FeatureCollector;
import com.onthegomap.flatmap.Parse;
import com.onthegomap.flatmap.Translations;
import com.onthegomap.flatmap.VectorTileEncoder;
import com.onthegomap.flatmap.monitoring.Stats;
import com.onthegomap.flatmap.openmaptiles.LanguageUtils;
import com.onthegomap.flatmap.openmaptiles.OpenMapTilesProfile;
import com.onthegomap.flatmap.openmaptiles.generated.OpenMapTilesSchema;
import com.onthegomap.flatmap.openmaptiles.generated.Tables;
import java.util.List;

public class MountainPeak implements
  OpenMapTilesSchema.MountainPeak,
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
        .setAttrs(LanguageUtils.getNames(element.source().properties(), translations))
        .setAttrs(elevationTags(meters))
        .setBufferPixels(BUFFER_SIZE)
        .setZorder(
          meters +
            (nullIfEmpty(element.wikipedia()) != null ? 10_000 : 0) +
            (nullIfEmpty(element.name()) != null ? 10_000 : 0)
        )
        .setZoomRange(7, 14)
        .setLabelGridSizeAndLimit(13, 100, 5);
    }
  }

  @Override
  public List<VectorTileEncoder.Feature> postProcess(int zoom, List<VectorTileEncoder.Feature> items) {
    LongIntMap groupCounts = new LongIntHashMap();
    for (int i = items.size() - 1; i >= 0; i--) {
      VectorTileEncoder.Feature feature = items.get(i);
      int gridrank = groupCounts.getOrDefault(feature.group(), 1);
      groupCounts.put(feature.group(), gridrank + 1);
      if (!feature.attrs().containsKey(Fields.RANK)) {
        feature.attrs().put(Fields.RANK, gridrank);
      }
    }
    return items;
  }
}
