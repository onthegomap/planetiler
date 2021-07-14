package com.onthegomap.flatmap.openmaptiles.layers;

import static com.onthegomap.flatmap.collections.FeatureGroup.Z_ORDER_MAX;
import static com.onthegomap.flatmap.openmaptiles.Utils.coalesce;
import static com.onthegomap.flatmap.openmaptiles.Utils.nullIfEmpty;
import static com.onthegomap.flatmap.openmaptiles.Utils.nullOrEmpty;

import com.onthegomap.flatmap.Arguments;
import com.onthegomap.flatmap.FeatureCollector;
import com.onthegomap.flatmap.Parse;
import com.onthegomap.flatmap.SourceFeature;
import com.onthegomap.flatmap.Translations;
import com.onthegomap.flatmap.geo.GeoUtils;
import com.onthegomap.flatmap.geo.GeometryException;
import com.onthegomap.flatmap.geo.PolygonIndex;
import com.onthegomap.flatmap.monitoring.Stats;
import com.onthegomap.flatmap.openmaptiles.LanguageUtils;
import com.onthegomap.flatmap.openmaptiles.OpenMapTilesProfile;
import com.onthegomap.flatmap.openmaptiles.generated.OpenMapTilesSchema;
import com.onthegomap.flatmap.openmaptiles.generated.Tables;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.DoubleStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Place implements
  OpenMapTilesSchema.Place,
  OpenMapTilesProfile.NaturalEarthProcessor,
  Tables.OsmContinentPoint.Handler,
  Tables.OsmCountryPoint.Handler,
  Tables.OsmStatePoint.Handler,
  Tables.OsmIslandPoint.Handler,
  Tables.OsmIslandPolygon.Handler {

  private static final Logger LOGGER = LoggerFactory.getLogger(Place.class);

  private final Translations translations;

  private static record NaturalEarthRegion(String name, int rank) {

    NaturalEarthRegion(String name, int maxRank, double... ranks) {
      this(name, (int) Math.ceil(DoubleStream.of(ranks).average().orElse(maxRank)));
    }
  }

  private PolygonIndex<NaturalEarthRegion> countries = PolygonIndex.create();
  private PolygonIndex<NaturalEarthRegion> states = PolygonIndex.create();

  public Place(Translations translations, Arguments args, Stats stats) {
    this.translations = translations;
  }

  @Override
  public void processNaturalEarth(String table, SourceFeature feature, FeatureCollector features) {
    try {
      switch (table) {
        case "ne_10m_admin_0_countries" -> countries.put(feature.worldGeometry(), new NaturalEarthRegion(
          feature.getString("name"), 6,
          feature.getLong("scalerank"),
          feature.getLong("labelrank")
        ));
        case "ne_10m_admin_1_states_provinces" -> {
          Double scalerank = Parse.parseDoubleOrNull(feature.getTag("scalerank"));
          Double labelrank = Parse.parseDoubleOrNull(feature.getTag("labelrank"));
          if (scalerank != null && scalerank <= 3 && labelrank != null && labelrank <= 2) {
            states.put(feature.worldGeometry(), new NaturalEarthRegion(
              feature.getString("name"), 6,
              scalerank,
              labelrank,
              feature.getLong("datarank")
            ));
          }
        }
      }
    } catch (GeometryException e) {
      LOGGER
        .warn("Error getting geometry for natural earth feature " + table + " " + feature.getTag("ogc_fid") + " " + e);
    }
  }

  @Override
  public void process(Tables.OsmContinentPoint element, FeatureCollector features) {
    if (!nullOrEmpty(element.name())) {
      features.point(LAYER_NAME).setBufferPixels(BUFFER_SIZE)
        .setAttrs(LanguageUtils.getNames(element.source().properties(), translations))
        .setAttr(Fields.CLASS, FieldValues.CLASS_CONTINENT)
        .setAttr(Fields.RANK, 1)
        .setAttrs(LanguageUtils.getNames(element.source().properties(), translations))
        .setZoomRange(0, 3);
    }
  }

  @Override
  public void process(Tables.OsmCountryPoint element, FeatureCollector features) {
    if (nullOrEmpty(element.name())) {
      return;
    }
    String isoA2 = coalesce(
      nullIfEmpty(element.countryCodeIso31661Alpha2()),
      nullIfEmpty(element.iso31661Alpha2()),
      nullIfEmpty(element.iso31661())
    );
    if (isoA2 == null) {
      return;
    }
    try {
      int rank = 7;
      NaturalEarthRegion country = countries.get(element.source().worldGeometry().getCentroid());
      var names = LanguageUtils.getNames(element.source().properties(), translations);

      if (country != null) {
        if (nullOrEmpty(names.get(Fields.NAME_EN))) {
          names.put(Fields.NAME_EN, country.name);
        }
        rank = country.rank;
      }

      rank = Math.min(6, Math.max(1, rank));

      features.point(LAYER_NAME).setBufferPixels(BUFFER_SIZE)
        .setAttrs(names)
        .setAttr(Fields.ISO_A2, isoA2)
        .setAttr(Fields.CLASS, FieldValues.CLASS_COUNTRY)
        .setAttr(Fields.RANK, rank)
        .setZoomRange(rank - 1, 14)
        .setZorder(-rank);
    } catch (GeometryException e) {
      LOGGER.warn("Unable to get point for OSM country " + element.source().id());
    }
  }

  @Override
  public void process(Tables.OsmStatePoint element, FeatureCollector features) {
    try {
      // don't want nearest since we pre-filter the states in the polygon index
      NaturalEarthRegion state = states.getOnlyContaining(element.source().worldGeometry().getCentroid());
      if (state != null) {
        var names = LanguageUtils.getNames(element.source().properties(), translations);
        if (nullOrEmpty(names.get(Fields.NAME_EN))) {
          names.put(Fields.NAME_EN, state.name);
        }
        int rank = Math.min(6, Math.max(1, state.rank));

        features.point(LAYER_NAME).setBufferPixels(BUFFER_SIZE)
          .setAttrs(names)
          .setAttr(Fields.CLASS, FieldValues.CLASS_STATE)
          .setAttr(Fields.RANK, rank)
          .setZoomRange(2, 14)
          .setZorder(-rank);
      }
    } catch (GeometryException e) {
      LOGGER.warn("Unable to get point for OSM state " + element.source().id());
    }
  }

  private static double squareMeters(double meters) {
    double oneSideMeters = Math.sqrt(meters);
    double oneSideWorld = GeoUtils.metersToPixelAtEquator(0, oneSideMeters) / 256d;
    return Math.pow(oneSideWorld, 2);
  }

  private static final TreeMap<Double, Integer> ISLAND_AREA_RANKS = new TreeMap<>(Map.of(
    Double.MAX_VALUE, 3,
    squareMeters(40_000_000), 4,
    squareMeters(15_000_000), 5,
    squareMeters(1_000_000), 6
  ));

  private static final int ISLAND_ZORDER_RANGE = Z_ORDER_MAX;
  private static final double ISLAND_LOG_AREA_RANGE = Math.log(Math.pow(4, 26)); // 2^14 tiles, 2^12 pixels per tile

  @Override
  public void process(Tables.OsmIslandPolygon element, FeatureCollector features) {
    try {
      double area = element.source().area();
      int rank = ISLAND_AREA_RANKS.ceilingEntry(area).getValue();
      int minzoom = rank <= 3 ? 8 : rank <= 4 ? 9 : 10;

      // set z-order based on log(area)
      double logWorldArea = Math
        .min(1d, Math.max(0d, (Math.log(area) + ISLAND_LOG_AREA_RANGE) / ISLAND_LOG_AREA_RANGE));
      int zOrder = (int) (logWorldArea * ISLAND_ZORDER_RANGE);

      features.pointOnSurface(LAYER_NAME).setBufferPixels(BUFFER_SIZE)
        .setAttrs(LanguageUtils.getNames(element.source().properties(), translations))
        .setAttr(Fields.CLASS, "island")
        .setAttr(Fields.RANK, rank)
        .setZoomRange(minzoom, 14)
        .setZorder(zOrder);
    } catch (GeometryException e) {
      LOGGER.warn("Unable to get area for OSM island polygon " + element.source().id() + ": " + e);
    }
  }

  @Override
  public void process(Tables.OsmIslandPoint element, FeatureCollector features) {
    features.point(LAYER_NAME).setBufferPixels(BUFFER_SIZE)
      .setAttrs(LanguageUtils.getNames(element.source().properties(), translations))
      .setAttr(Fields.CLASS, "island")
      .setAttr(Fields.RANK, 7)
      .setZoomRange(12, 14);
  }
}
