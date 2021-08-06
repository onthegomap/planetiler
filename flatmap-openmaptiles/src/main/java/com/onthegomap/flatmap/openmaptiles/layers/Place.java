/*
Copyright (c) 2016, KlokanTech.com & OpenMapTiles contributors.
All rights reserved.

Code license: BSD 3-Clause License

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:

* Redistributions of source code must retain the above copyright notice, this
  list of conditions and the following disclaimer.

* Redistributions in binary form must reproduce the above copyright notice,
  this list of conditions and the following disclaimer in the documentation
  and/or other materials provided with the distribution.

* Neither the name of the copyright holder nor the names of its
  contributors may be used to endorse or promote products derived from
  this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

Design license: CC-BY 4.0

See https://github.com/openmaptiles/openmaptiles/blob/master/LICENSE.md for details on usage
*/
package com.onthegomap.flatmap.openmaptiles.layers;

import static com.onthegomap.flatmap.collection.FeatureGroup.Z_ORDER_BITS;
import static com.onthegomap.flatmap.collection.FeatureGroup.Z_ORDER_MAX;
import static com.onthegomap.flatmap.collection.FeatureGroup.Z_ORDER_MIN;
import static com.onthegomap.flatmap.openmaptiles.Utils.coalesce;
import static com.onthegomap.flatmap.openmaptiles.Utils.nullIfEmpty;
import static com.onthegomap.flatmap.openmaptiles.Utils.nullOrEmpty;

import com.carrotsearch.hppc.LongIntHashMap;
import com.carrotsearch.hppc.LongIntMap;
import com.onthegomap.flatmap.FeatureCollector;
import com.onthegomap.flatmap.Translations;
import com.onthegomap.flatmap.VectorTileEncoder;
import com.onthegomap.flatmap.config.Arguments;
import com.onthegomap.flatmap.geo.GeoUtils;
import com.onthegomap.flatmap.geo.GeometryException;
import com.onthegomap.flatmap.geo.PointIndex;
import com.onthegomap.flatmap.geo.PolygonIndex;
import com.onthegomap.flatmap.openmaptiles.LanguageUtils;
import com.onthegomap.flatmap.openmaptiles.OpenMapTilesProfile;
import com.onthegomap.flatmap.openmaptiles.generated.OpenMapTilesSchema;
import com.onthegomap.flatmap.openmaptiles.generated.Tables;
import com.onthegomap.flatmap.reader.SourceFeature;
import com.onthegomap.flatmap.stats.Stats;
import com.onthegomap.flatmap.util.Parse;
import com.onthegomap.flatmap.util.ZoomFunction;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;
import java.util.stream.DoubleStream;
import java.util.stream.Stream;
import org.apache.commons.lang3.StringUtils;
import org.locationtech.jts.geom.Point;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is ported to Java from https://github.com/openmaptiles/openmaptiles/tree/master/layers/place
 */
public class Place implements
  OpenMapTilesSchema.Place,
  OpenMapTilesProfile.NaturalEarthProcessor,
  Tables.OsmContinentPoint.Handler,
  Tables.OsmCountryPoint.Handler,
  Tables.OsmStatePoint.Handler,
  Tables.OsmIslandPoint.Handler,
  Tables.OsmIslandPolygon.Handler,
  Tables.OsmCityPoint.Handler,
  OpenMapTilesProfile.FeaturePostProcessor {

  private static final Logger LOGGER = LoggerFactory.getLogger(Place.class);

  private final Translations translations;
  private final Stats stats;

  private static record NaturalEarthRegion(String name, int rank) {

    NaturalEarthRegion(String name, int maxRank, double... ranks) {
      this(name, (int) Math.ceil(DoubleStream.of(ranks).average().orElse(maxRank)));
    }
  }

  private static record NaturalEarthPoint(String name, String wikidata, int scaleRank, Set<String> names) {}

  private PolygonIndex<NaturalEarthRegion> countries = PolygonIndex.create();
  private PolygonIndex<NaturalEarthRegion> states = PolygonIndex.create();
  private PointIndex<NaturalEarthPoint> cities = PointIndex.create();

  @Override
  public void release() {
    countries = null;
    states = null;
    cities = null;
  }

  public Place(Translations translations, Arguments args, Stats stats) {
    this.translations = translations;
    this.stats = stats;
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
        case "ne_10m_populated_places" -> cities.put(feature.worldGeometry(), new NaturalEarthPoint(
          feature.getString("name"),
          feature.getString("wikidataid"),
          (int) feature.getLong("scalerank"),
          Stream.of("name", "namealt", "meganame", "gn_ascii", "nameascii").map(feature::getString)
            .filter(Objects::nonNull)
            .map(s -> s.toLowerCase(Locale.ROOT))
            .collect(Collectors.toSet())
        ));
      }
    } catch (GeometryException e) {
      e.log(stats, "omt_place_ne",
        "Error getting geometry for natural earth feature " + table + " " + feature.getTag("ogc_fid"));
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
      e.log(stats, "omt_place_country",
        "Unable to get point for OSM country " + element.source().id());
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
      e.log(stats, "omt_place_state",
        "Unable to get point for OSM state " + element.source().id());
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
      e.log(stats, "omt_place_island_poly",
        "Unable to get point for OSM island polygon " + element.source().id());
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

  private static final Set<String> majorCityPlaces = Set.of("city", "town", "village");
  private static final double CITY_JOIN_DISTANCE = GeoUtils.metersToPixelAtEquator(0, 50_000) / 256d;

  enum PlaceType {
    CITY("city"),
    TOWN("town"),
    VILLAGE("village"),
    HAMLET("hamlet"),
    SUBURB("suburb"),
    QUARTER("quarter"),
    NEIGHBORHOOD("neighbourhood"),
    ISOLATED_DWELLING("isolated_dwelling"),
    UNKNOWN("unknown");

    private final String name;
    private static final Map<String, PlaceType> byName = new HashMap<>();

    static {
      for (PlaceType place : values()) {
        byName.put(place.name, place);
      }
    }

    PlaceType(String name) {
      this.name = name;
    }

    public static PlaceType forName(String name) {
      return byName.getOrDefault(name, UNKNOWN);
    }
  }

  private static final int Z_ORDER_RANK_BITS = 4;
  private static final int Z_ORDER_PLACE_BITS = 4;
  private static final int Z_ORDER_LENGTH_BITS = 5;
  private static final int Z_ORDER_POPULATION_BITS = Z_ORDER_BITS -
    (Z_ORDER_RANK_BITS + Z_ORDER_PLACE_BITS + Z_ORDER_LENGTH_BITS);
  private static final int Z_ORDER_POPULATION_RANGE = (1 << Z_ORDER_POPULATION_BITS) - 1;
  private static final double LOG_MAX_POPULATION = Math.log(100_000_000d);

  // order by rank asc, place asc, population desc, name.length asc
  static int getZorder(Integer rank, PlaceType place, long population, String name) {
    int zorder = rank == null ? 0 : Math.max(1, 15 - rank);
    zorder = (zorder << Z_ORDER_PLACE_BITS) | (place == null ? 0 : Math.max(1, 15 - place.ordinal()));
    double logPop = Math.min(LOG_MAX_POPULATION, Math.log(population));
    zorder = (zorder << Z_ORDER_POPULATION_BITS) | Math.max(0, Math.min(Z_ORDER_POPULATION_RANGE,
      (int) Math.round(logPop * Z_ORDER_POPULATION_RANGE / LOG_MAX_POPULATION)));
    zorder = (zorder << Z_ORDER_LENGTH_BITS) | (name == null ? 0 : Math.max(1, 31 - name.length()));

    return zorder + Z_ORDER_MIN;
  }

  private static final ZoomFunction<Number> LABEL_GRID_LIMITS = ZoomFunction.fromMaxZoomThresholds(Map.of(
    8, 4,
    9, 8,
    10, 12,
    12, 14
  ), 0);

  @Override
  public void process(Tables.OsmCityPoint element, FeatureCollector features) {
    Integer rank = null;
    if (majorCityPlaces.contains(element.place())) {
      try {
        Point point = element.source().worldGeometry().getCentroid();
        List<NaturalEarthPoint> neCities = cities.getWithin(point, CITY_JOIN_DISTANCE);
        String rawName = coalesce(element.name(), "");
        String name = coalesce(rawName, "").toLowerCase(Locale.ROOT);
        String nameEn = coalesce(element.nameEn(), "").toLowerCase(Locale.ROOT);
        String normalizedName = StringUtils.stripAccents(rawName);
        String wikidata = element.source().getString("wikidata", "");
        for (var neCity : neCities) {
          if (wikidata.equals(neCity.wikidata) ||
            neCity.names.contains(name) ||
            neCity.names.contains(nameEn) ||
            normalizedName.equals(neCity.name)) {
            rank = neCity.scaleRank <= 5 ? neCity.scaleRank + 1 : neCity.scaleRank;
            break;
          }
        }
      } catch (GeometryException e) {
        e.log(stats, "omt_place_city",
          "Unable to get point for OSM city " + element.source().id());
      }
    }

    String capital = element.capital();

    PlaceType placeType = PlaceType.forName(element.place());

    int minzoom = rank != null && rank == 1 ? 2 :
      rank != null && rank <= 8 ? Math.max(3, rank - 1) :
        placeType.ordinal() <= PlaceType.TOWN.ordinal() ? 7 :
          placeType.ordinal() <= PlaceType.VILLAGE.ordinal() ? 8 :
            placeType.ordinal() <= PlaceType.SUBURB.ordinal() ? 11 : 14;

    var feature = features.point(LAYER_NAME).setBufferPixels(BUFFER_SIZE)
      .setAttrs(LanguageUtils.getNames(element.source().properties(), translations))
      .setAttr(Fields.CLASS, element.place())
      .setAttr(Fields.RANK, rank)
      .setZoomRange(minzoom, 14)
      .setZorder(getZorder(rank, placeType, element.population(), element.name()))
      .setLabelGridPixelSize(12, 128);

    if (rank == null) {
      feature.setLabelGridLimitFunction(LABEL_GRID_LIMITS);
    }

    if ("2".equals(capital) || "yes".equals(capital)) {
      feature.setAttr(Fields.CAPITAL, 2);
    } else if ("4".equals(capital)) {
      feature.setAttr(Fields.CAPITAL, 4);
    }
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
        feature.attrs().put(Fields.RANK, 10 + gridrank);
      }
    }
    return items;
  }
}
