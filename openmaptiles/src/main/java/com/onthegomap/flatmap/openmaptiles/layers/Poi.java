package com.onthegomap.flatmap.openmaptiles.layers;

import static com.onthegomap.flatmap.openmaptiles.Utils.coalesce;
import static com.onthegomap.flatmap.openmaptiles.Utils.nullIf;
import static com.onthegomap.flatmap.openmaptiles.Utils.nullIfEmpty;
import static com.onthegomap.flatmap.openmaptiles.Utils.nullOrEmpty;
import static java.util.Map.entry;

import com.carrotsearch.hppc.LongIntHashMap;
import com.carrotsearch.hppc.LongIntMap;
import com.onthegomap.flatmap.Arguments;
import com.onthegomap.flatmap.FeatureCollector;
import com.onthegomap.flatmap.Parse;
import com.onthegomap.flatmap.Translations;
import com.onthegomap.flatmap.VectorTileEncoder;
import com.onthegomap.flatmap.geo.GeometryException;
import com.onthegomap.flatmap.monitoring.Stats;
import com.onthegomap.flatmap.openmaptiles.LanguageUtils;
import com.onthegomap.flatmap.openmaptiles.MultiExpression;
import com.onthegomap.flatmap.openmaptiles.OpenMapTilesProfile;
import com.onthegomap.flatmap.openmaptiles.generated.OpenMapTilesSchema;
import com.onthegomap.flatmap.openmaptiles.generated.Tables;
import java.util.List;
import java.util.Map;

public class Poi implements OpenMapTilesSchema.Poi,
  Tables.OsmPoiPoint.Handler,
  Tables.OsmPoiPolygon.Handler,
  OpenMapTilesProfile.FeaturePostProcessor {

  private final MultiExpression.MultiExpressionIndex<String> classMapping;
  private final Translations translations;

  public Poi(Translations translations, Arguments args, Stats stats) {
    this.classMapping = FieldMappings.Class.index();
    this.translations = translations;
  }

  private String poiClass(String subclass, String mappingKey) {
    subclass = coalesce(subclass, "");
    return classMapping.getOrElse(Map.of(
      "subclass", subclass,
      "mapping_key", coalesce(mappingKey, "")
    ), subclass);
  }

  private static final Map<String, Integer> CLASS_RANKS = Map.ofEntries(
    entry(FieldValues.CLASS_HOSPITAL, 20),
    entry(FieldValues.CLASS_RAILWAY, 40),
    entry(FieldValues.CLASS_BUS, 50),
    entry(FieldValues.CLASS_ATTRACTION, 70),
    entry(FieldValues.CLASS_HARBOR, 75),
    entry(FieldValues.CLASS_COLLEGE, 80),
    entry(FieldValues.CLASS_SCHOOL, 85),
    entry(FieldValues.CLASS_STADIUM, 90),
    entry("zoo", 95),
    entry(FieldValues.CLASS_TOWN_HALL, 100),
    entry(FieldValues.CLASS_CAMPSITE, 110),
    entry(FieldValues.CLASS_CEMETERY, 115),
    entry(FieldValues.CLASS_PARK, 120),
    entry(FieldValues.CLASS_LIBRARY, 130),
    entry("police", 135),
    entry(FieldValues.CLASS_POST, 140),
    entry(FieldValues.CLASS_GOLF, 150),
    entry(FieldValues.CLASS_SHOP, 400),
    entry(FieldValues.CLASS_GROCERY, 500),
    entry(FieldValues.CLASS_FAST_FOOD, 600),
    entry(FieldValues.CLASS_CLOTHING_STORE, 700),
    entry(FieldValues.CLASS_BAR, 800)
  );

  private static int poiClassRank(String clazz) {
    return CLASS_RANKS.getOrDefault(clazz, 1_000);
  }

  private int minzoom(String subclass, String mappingKey) {
    boolean lowZoom = ("station".equals(subclass) && "railway".equals(mappingKey)) ||
      "halt".equals(subclass) || "ferry_terminal".equals(subclass);
    return lowZoom ? 12 : 14;
  }

  @Override
  public void process(Tables.OsmPoiPoint element, FeatureCollector features) {
    String rawSubclass = element.subclass();
    if ("station".equals(rawSubclass) && "subway".equals(element.station())) {
      rawSubclass = "subway";
    }
    if ("station".equals(rawSubclass) && "yes".equals(element.funicular())) {
      rawSubclass = "halt";
    }

    String subclass = switch (rawSubclass) {
      case "information" -> nullIfEmpty(element.information());
      case "place_of_worship" -> nullIfEmpty(element.religion());
      case "pitch" -> nullIfEmpty(element.sport());
      default -> rawSubclass;
    };
    String poiClass = poiClass(rawSubclass, element.mappingKey());
    int poiClassRank = poiClassRank(poiClass);
    int rankOrder = poiClassRank + ((nullOrEmpty(element.name())) ? 2000 : 0);

    features.point(LAYER_NAME).setBufferPixels(BUFFER_SIZE)
      .setAttr(Fields.CLASS, poiClass)
      .setAttr(Fields.SUBCLASS, subclass)
      .setAttr(Fields.LAYER, nullIf(element.layer(), 0))
      .setAttr(Fields.LEVEL, Parse.parseLongOrNull(element.source().getTag("level")))
      .setAttr(Fields.INDOOR, element.indoor() ? 1 : null)
      .setAttrs(LanguageUtils.getNames(element.source().properties(), translations))
      .setLabelGridPixelSize(14, 64)
      .setZorder(-rankOrder)
      .setZoomRange(minzoom(element.subclass(), element.mappingKey()), 14);
  }

  @Override
  public void process(Tables.OsmPoiPolygon element, FeatureCollector features) {
    // TODO duplicate code
    String rawSubclass = element.subclass();
    if ("station".equals(rawSubclass) && "subway".equals(element.station())) {
      rawSubclass = "subway";
    }
    if ("station".equals(rawSubclass) && "yes".equals(element.funicular())) {
      rawSubclass = "halt";
    }

    String subclass = switch (rawSubclass) {
      case "information" -> nullIfEmpty(element.information());
      case "place_of_worship" -> nullIfEmpty(element.religion());
      case "pitch" -> nullIfEmpty(element.sport());
      default -> rawSubclass;
    };
    String poiClass = poiClass(rawSubclass, element.mappingKey());
    int poiClassRank = poiClassRank(poiClass);
    int rankOrder = poiClassRank + ((nullOrEmpty(element.name())) ? 2000 : 0);

    // TODO pointOnSurface if not convex
    features.centroid(LAYER_NAME).setBufferPixels(BUFFER_SIZE)
      .setAttr(Fields.CLASS, poiClass)
      .setAttr(Fields.SUBCLASS, subclass)
      .setAttr(Fields.LAYER, nullIf(element.layer(), 0))
      .setAttr(Fields.LEVEL, Parse.parseLongOrNull(element.source().getTag("level")))
      .setAttr(Fields.INDOOR, element.indoor() ? 1 : null)
      .setAttrs(LanguageUtils.getNames(element.source().properties(), translations))
      .setLabelGridPixelSize(14, 64)
      .setZorder(-rankOrder)
      .setZoomRange(minzoom(element.subclass(), element.mappingKey()), 14);
  }

  @Override
  public List<VectorTileEncoder.Feature> postProcess(int zoom,
    List<VectorTileEncoder.Feature> items) throws GeometryException {
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
