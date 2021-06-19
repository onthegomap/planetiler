package com.onthegomap.flatmap.openmaptiles.layers;

import static com.onthegomap.flatmap.openmaptiles.Utils.nullIfEmpty;

import com.onthegomap.flatmap.Arguments;
import com.onthegomap.flatmap.FeatureCollector;
import com.onthegomap.flatmap.FeatureMerge;
import com.onthegomap.flatmap.SourceFeature;
import com.onthegomap.flatmap.Translations;
import com.onthegomap.flatmap.VectorTileEncoder;
import com.onthegomap.flatmap.ZoomFunction;
import com.onthegomap.flatmap.geo.GeometryException;
import com.onthegomap.flatmap.monitoring.Stats;
import com.onthegomap.flatmap.openmaptiles.LanguageUtils;
import com.onthegomap.flatmap.openmaptiles.OpenMapTilesProfile;
import com.onthegomap.flatmap.openmaptiles.Utils;
import com.onthegomap.flatmap.openmaptiles.generated.OpenMapTilesSchema;
import com.onthegomap.flatmap.openmaptiles.generated.Tables;
import java.util.List;
import java.util.Map;

public class Waterway implements OpenMapTilesSchema.Waterway, Tables.OsmWaterwayLinestring.Handler,
  OpenMapTilesProfile.FeaturePostProcessor, OpenMapTilesProfile.NaturalEarthProcessor {

  private final Translations translations;

  public Waterway(Translations translations, Arguments args, Stats stats) {
    this.translations = translations;
  }

  private static final Map<String, Integer> minzooms = Map.of(
    "river", 12,
    "canal", 12,

    "stream", 13,
    "drain", 13,
    "ditch", 13
  );

  private static final ZoomFunction.MeterThresholds minPixelSizeThresholds = ZoomFunction.meterThresholds()
    .put(9, 8_000)
    .put(10, 4_000)
    .put(11, 1_000);

  @Override
  public void process(Tables.OsmWaterwayLinestring element, FeatureCollector features) {
    String waterway = element.waterway();
    String name = nullIfEmpty(element.name());
    boolean important = "river".equals(waterway) && name != null;
    int minzoom = important ? 9 : minzooms.getOrDefault(element.waterway(), 14);
    features.line(LAYER_NAME)
      .setBufferPixels(BUFFER_SIZE)
      .setAttr(Fields.CLASS, element.waterway())
      .setAttrs(LanguageUtils.getNames(element.source().properties(), translations))
      .setZoomRange(minzoom, 14)
      // details only at higher zoom levels
      .setAttrWithMinzoom(Fields.BRUNNEL, Utils.brunnel(element.isBridge(), element.isTunnel()), 12)
      .setAttrWithMinzoom(Fields.INTERMITTENT, element.isIntermittent() ? 1 : 0, 12)
      // at lower zoom levels, we'll merge linestrings and limit length/clip afterwards
      .setBufferPixelOverrides(minPixelSizeThresholds).setMinPixelSizeBelowZoom(11, 0);
  }

  @Override
  public void processNaturalEarth(String table, SourceFeature feature, FeatureCollector features) {
    if (feature.hasTag("featurecla", "River")) {
      record ZoomRange(int min, int max) {}
      ZoomRange zoom = switch (table) {
        case "ne_10m_rivers_lake_centerlines" -> new ZoomRange(6, 8);
        case "ne_50m_rivers_lake_centerlines" -> new ZoomRange(4, 5);
        case "ne_110m_rivers_lake_centerlines" -> new ZoomRange(3, 3);
        default -> null;
      };
      if (zoom != null) {
        features.line(LAYER_NAME)
          .setBufferPixels(BUFFER_SIZE)
          .setAttr(Fields.CLASS, "river")
          .setZoomRange(zoom.min, zoom.max);
      }
    }
  }

  @Override
  public List<VectorTileEncoder.Feature> postProcess(int zoom, List<VectorTileEncoder.Feature> items)
    throws GeometryException {
    if (zoom >= 9 && zoom <= 11) {
      return FeatureMerge.mergeLineStrings(items, minPixelSizeThresholds.apply(zoom).doubleValue(), 0.1d, BUFFER_SIZE);
    }
    return items;
  }
}
