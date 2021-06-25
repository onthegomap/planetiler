package com.onthegomap.flatmap.openmaptiles.layers;

import static com.onthegomap.flatmap.collections.FeatureGroup.Z_ORDER_BITS;
import static com.onthegomap.flatmap.collections.FeatureGroup.Z_ORDER_MIN;
import static com.onthegomap.flatmap.openmaptiles.Utils.coalesce;
import static com.onthegomap.flatmap.openmaptiles.Utils.nullIfEmpty;

import com.carrotsearch.hppc.LongIntHashMap;
import com.carrotsearch.hppc.LongIntMap;
import com.onthegomap.flatmap.Arguments;
import com.onthegomap.flatmap.FeatureCollector;
import com.onthegomap.flatmap.GeometryType;
import com.onthegomap.flatmap.Translations;
import com.onthegomap.flatmap.VectorTileEncoder;
import com.onthegomap.flatmap.geo.GeoUtils;
import com.onthegomap.flatmap.geo.GeometryException;
import com.onthegomap.flatmap.monitoring.Stats;
import com.onthegomap.flatmap.openmaptiles.LanguageUtils;
import com.onthegomap.flatmap.openmaptiles.OpenMapTilesProfile;
import com.onthegomap.flatmap.openmaptiles.generated.OpenMapTilesSchema;
import com.onthegomap.flatmap.openmaptiles.generated.Tables;
import java.util.List;
import java.util.Locale;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Park implements
  OpenMapTilesSchema.Park,
  Tables.OsmParkPolygon.Handler,
  OpenMapTilesProfile.FeaturePostProcessor {

  private static final Logger LOGGER = LoggerFactory.getLogger(Park.class);
  private final Translations translations;

  public Park(Translations translations, Arguments args, Stats stats) {
    this.translations = translations;
  }

  private static final int PARK_NATIONAL_PARK_BOOST = 1 << (Z_ORDER_BITS - 1);
  private static final int PARK_WIKIPEDIA_BOOST = 1 << (Z_ORDER_BITS - 2);
  private static final double WORLD_AREA_FOR_70K_SQUARE_METERS =
    Math.pow(GeoUtils.metersToPixelAtEquator(0, Math.sqrt(70_000)) / 256d, 2);
  private static final double LOG2 = Math.log(2);
  private static final int PARK_AREA_RANGE = 1 << (Z_ORDER_BITS - 3);
  private static final double PARK_LOG_RANGE = Math.log(Math.pow(4, 26)); // 2^14 tiles, 2^12 pixels per tile
  private static final double LOG4 = Math.log(4);

  @Override
  public void process(Tables.OsmParkPolygon element, FeatureCollector features) {
    String protectionTitle = element.protectionTitle();
    if (protectionTitle != null) {
      protectionTitle = protectionTitle.replace(' ', '_').toLowerCase(Locale.ROOT);
    }
    String clazz = coalesce(
      nullIfEmpty(protectionTitle),
      nullIfEmpty(element.boundary()),
      nullIfEmpty(element.leisure())
    );
    features.polygon(LAYER_NAME).setBufferPixels(BUFFER_SIZE)
      .setAttr(Fields.CLASS, clazz)
      .setMinPixelSize(2)
      .setZoomRange(6, 14);

    if (element.name() != null) {
      try {
        double area = element.source().area();
        int minzoom = (int) Math.floor(20 - Math.log(area / WORLD_AREA_FOR_70K_SQUARE_METERS) / LOG2);
        double logWorldArea = Math.min(1d, Math.max(0d, (Math.log(area) + PARK_LOG_RANGE) / PARK_LOG_RANGE));
        int areaBoost = (int) (logWorldArea * PARK_AREA_RANGE);
        minzoom = Math.min(14, Math.max(6, minzoom));

        features.centroid(LAYER_NAME).setBufferPixels(256)
          .setAttr(Fields.CLASS, clazz)
          .setAttrs(LanguageUtils.getNames(element.source().properties(), translations))
          .setLabelGridPixelSize(14, 100)
          .setZorder(Z_ORDER_MIN +
            ("national_park".equals(clazz) ? PARK_NATIONAL_PARK_BOOST : 0) +
            ((element.source().hasTag("wikipedia") || element.source().hasTag("wikidata")) ? PARK_WIKIPEDIA_BOOST : 0) +
            areaBoost
          ).setZoomRange(minzoom, 14);
      } catch (GeometryException e) {
        LOGGER.warn("Unable to get park area for " + element.source().id() + ": " + e.getMessage());
      }
    }
  }

  @Override
  public List<VectorTileEncoder.Feature> postProcess(int zoom, List<VectorTileEncoder.Feature> items) {
    LongIntMap counts = new LongIntHashMap();
    for (int i = items.size() - 1; i >= 0; i--) {
      var feature = items.get(i);
      if (feature.geometry().geomType() == GeometryType.POINT && feature.hasGroup()) {
        int count = counts.getOrDefault(feature.group(), 0) + 1;
        feature.attrs().put("rank", count);
        counts.put(feature.group(), count);
      }
    }
    return items;
  }
}
