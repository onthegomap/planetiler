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
package com.onthegomap.planetiler.basemap.layers;

import static com.onthegomap.planetiler.basemap.util.Utils.coalesce;
import static com.onthegomap.planetiler.basemap.util.Utils.nullIfEmpty;
import static com.onthegomap.planetiler.basemap.util.Utils.nullOrEmpty;
import static com.onthegomap.planetiler.collection.FeatureGroup.SORT_KEY_BITS;

import com.carrotsearch.hppc.LongIntHashMap;
import com.carrotsearch.hppc.LongIntMap;
import com.onthegomap.planetiler.FeatureCollector;
import com.onthegomap.planetiler.VectorTile;
import com.onthegomap.planetiler.basemap.BasemapProfile;
import com.onthegomap.planetiler.basemap.generated.OpenMapTilesSchema;
import com.onthegomap.planetiler.basemap.generated.Tables;
import com.onthegomap.planetiler.basemap.util.LanguageUtils;
import com.onthegomap.planetiler.config.PlanetilerConfig;
import com.onthegomap.planetiler.geo.GeoUtils;
import com.onthegomap.planetiler.geo.GeometryException;
import com.onthegomap.planetiler.geo.PointIndex;
import com.onthegomap.planetiler.geo.PolygonIndex;
import com.onthegomap.planetiler.reader.SourceFeature;
import com.onthegomap.planetiler.stats.Stats;
import com.onthegomap.planetiler.util.Parse;
import com.onthegomap.planetiler.util.SortKey;
import com.onthegomap.planetiler.util.Translations;
import com.onthegomap.planetiler.util.ZoomFunction;
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

/**
 * Defines the logic for generating label points for populated places like continents, countries, cities, and towns in
 * the {@code place} layer from source features.
 * <p>
 * This class is ported to Java from <a href="https://github.com/openmaptiles/openmaptiles/tree/master/layers/place">OpenMapTiles
 * place sql files</a>.
 */
public class Place implements
  OpenMapTilesSchema.Place,
  BasemapProfile.NaturalEarthProcessor,
  Tables.OsmContinentPoint.Handler,
  Tables.OsmCountryPoint.Handler,
  Tables.OsmStatePoint.Handler,
  Tables.OsmIslandPoint.Handler,
  Tables.OsmIslandPolygon.Handler,
  Tables.OsmCityPoint.Handler,
  BasemapProfile.FeaturePostProcessor {

  /*
   * Place labels locations and names come from OpenStreetMap, but we also join with natural
   * earth state/country geographic areas and city point labels to give a hint for what rank
   * and minimum zoom level to use for those points.
   */

  private static final TreeMap<Double, Integer> ISLAND_AREA_RANKS = new TreeMap<>(Map.of(
    Double.MAX_VALUE, 3,
    squareMetersToWorldArea(40_000_000), 4,
    squareMetersToWorldArea(15_000_000), 5,
    squareMetersToWorldArea(1_000_000), 6
  ));
  private static final double MIN_ISLAND_WORLD_AREA = Math.pow(4, -26); // 2^14 tiles, 2^12 pixels per tile
  private static final double CITY_JOIN_DISTANCE = GeoUtils.metersToPixelAtEquator(0, 50_000) / 256d;
  // constants for packing place label precedence into the sort-key field
  private static final double MAX_CITY_POPULATION = 100_000_000d;
  private static final Set<String> MAJOR_CITY_PLACES = Set.of("city", "town", "village");
  private static final ZoomFunction<Number> LABEL_GRID_LIMITS = ZoomFunction.fromMaxZoomThresholds(Map.of(
    8, 4,
    9, 8,
    10, 12,
    12, 14
  ), 0);
  private final Translations translations;
  private final Stats stats;
  // spatial indexes for joining natural earth place labels with their corresponding points
  // from openstreetmap
  private PolygonIndex<NaturalEarthRegion> countries = PolygonIndex.create();
  private PolygonIndex<NaturalEarthRegion> states = PolygonIndex.create();
  private PointIndex<NaturalEarthPoint> cities = PointIndex.create();

  public Place(Translations translations, PlanetilerConfig config, Stats stats) {
    this.translations = translations;
    this.stats = stats;
  }

  /** Returns the portion of the world that {@code squareMeters} covers where 1 is the entire planet. */
  private static double squareMetersToWorldArea(double squareMeters) {
    double oneSideMeters = Math.sqrt(squareMeters);
    double oneSideWorld = GeoUtils.metersToPixelAtEquator(0, oneSideMeters) / 256d;
    return Math.pow(oneSideWorld, 2);
  }

  /**
   * Packs place precedence ordering ({@code rank asc, place asc, population desc, name.length asc}) into an integer for
   * the sort-key field.
   */
  static int getSortKey(Integer rank, PlaceType place, long population, String name) {
    return SortKey
      // ORDER BY "rank" ASC NULLS LAST,
      .orderByInt(rank == null ? 15 : rank, 0, 15) // 4 bits
      // place ASC NULLS LAST,
      .thenByInt(place == null ? 15 : place.ordinal(), 0, 15)  // 4 bits
      // population DESC NULLS LAST,
      .thenByLog(population, MAX_CITY_POPULATION, 1, 1 << (SORT_KEY_BITS - 13) - 1)
      // length(name) ASC
      .thenByInt(name == null ? 0 : name.length(), 0, 31)  // 5 bits
      .get();
  }

  @Override
  public void release() {
    countries = null;
    states = null;
    cities = null;
  }

  @Override
  public void processNaturalEarth(String table, SourceFeature feature, FeatureCollector features) {
    // store data from natural earth to help with ranks and min zoom levels when actually
    // emitting features from openstreetmap data.
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
        .putAttrs(LanguageUtils.getNames(element.source().tags(), translations))
        .setAttr(Fields.CLASS, FieldValues.CLASS_CONTINENT)
        .setAttr(Fields.RANK, 1)
        .putAttrs(LanguageUtils.getNames(element.source().tags(), translations))
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
      // set country rank to 6, unless there is a match in natural earth that indicates it
      // should be lower
      int rank = 7;
      NaturalEarthRegion country = countries.get(element.source().worldGeometry().getCentroid());
      var names = LanguageUtils.getNames(element.source().tags(), translations);

      if (country != null) {
        if (nullOrEmpty(names.get(Fields.NAME_EN))) {
          names.put(Fields.NAME_EN, country.name);
        }
        rank = country.rank;
      }

      rank = Math.min(6, Math.max(1, rank));

      features.point(LAYER_NAME).setBufferPixels(BUFFER_SIZE)
        .putAttrs(names)
        .setAttr(Fields.ISO_A2, isoA2)
        .setAttr(Fields.CLASS, FieldValues.CLASS_COUNTRY)
        .setAttr(Fields.RANK, rank)
        .setMinZoom(rank - 1)
        .setSortKey(rank);
    } catch (GeometryException e) {
      e.log(stats, "omt_place_country",
        "Unable to get point for OSM country " + element.source().id());
    }
  }

  @Override
  public void process(Tables.OsmStatePoint element, FeatureCollector features) {
    try {
      // want the containing (not nearest) state polygon since we pre-filter the states in the polygon index
      // use natural earth to filter out any spurious states, and to set the rank field
      NaturalEarthRegion state = states.getOnlyContaining(element.source().worldGeometry().getCentroid());
      if (state != null) {
        var names = LanguageUtils.getNames(element.source().tags(), translations);
        if (nullOrEmpty(names.get(Fields.NAME_EN))) {
          names.put(Fields.NAME_EN, state.name);
        }
        int rank = Math.min(6, Math.max(1, state.rank));

        features.point(LAYER_NAME).setBufferPixels(BUFFER_SIZE)
          .putAttrs(names)
          .setAttr(Fields.CLASS, FieldValues.CLASS_STATE)
          .setAttr(Fields.RANK, rank)
          .setMinZoom(2)
          .setSortKey(rank);
      }
    } catch (GeometryException e) {
      e.log(stats, "omt_place_state",
        "Unable to get point for OSM state " + element.source().id());
    }
  }

  @Override
  public void process(Tables.OsmIslandPolygon element, FeatureCollector features) {
    try {
      double area = element.source().area();
      int rank = ISLAND_AREA_RANKS.ceilingEntry(area).getValue();
      int minzoom = rank <= 3 ? 8 : rank <= 4 ? 9 : 10;

      features.pointOnSurface(LAYER_NAME).setBufferPixels(BUFFER_SIZE)
        .putAttrs(LanguageUtils.getNames(element.source().tags(), translations))
        .setAttr(Fields.CLASS, "island")
        .setAttr(Fields.RANK, rank)
        .setMinZoom(minzoom)
        .setSortKey(SortKey.orderByLog(area, 1d, MIN_ISLAND_WORLD_AREA).get());
    } catch (GeometryException e) {
      e.log(stats, "omt_place_island_poly",
        "Unable to get point for OSM island polygon " + element.source().id());
    }
  }

  @Override
  public void process(Tables.OsmIslandPoint element, FeatureCollector features) {
    features.point(LAYER_NAME).setBufferPixels(BUFFER_SIZE)
      .putAttrs(LanguageUtils.getNames(element.source().tags(), translations))
      .setAttr(Fields.CLASS, "island")
      .setAttr(Fields.RANK, 7)
      .setMinZoom(12);
  }

  @Override
  public void process(Tables.OsmCityPoint element, FeatureCollector features) {
    Integer rank = null;
    if (MAJOR_CITY_PLACES.contains(element.place())) {
      // only for major cities, attempt to find a nearby natural earth label with a similar
      // name and use that to set a rank from OSM that causes the label to be shown at lower
      // zoom levels
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
      .putAttrs(LanguageUtils.getNames(element.source().tags(), translations))
      .setAttr(Fields.CLASS, element.place())
      .setAttr(Fields.RANK, rank)
      .setMinZoom(minzoom)
      .setSortKey(getSortKey(rank, placeType, element.population(), element.name()))
      .setLabelGridPixelSize(12, 128);

    if (rank == null) {
      feature.setLabelGridLimit(LABEL_GRID_LIMITS);
    }

    if ("2".equals(capital) || "yes".equals(capital)) {
      feature.setAttr(Fields.CAPITAL, 2);
    } else if ("4".equals(capital)) {
      feature.setAttr(Fields.CAPITAL, 4);
    }
  }

  @Override
  public List<VectorTile.Feature> postProcess(int zoom, List<VectorTile.Feature> items) {
    // infer the rank field from ordering of the place labels with each label grid square
    LongIntMap groupCounts = new LongIntHashMap();
    for (VectorTile.Feature feature : items) {
      int gridrank = groupCounts.getOrDefault(feature.group(), 1);
      groupCounts.put(feature.group(), gridrank + 1);
      if (!feature.attrs().containsKey(Fields.RANK)) {
        feature.attrs().put(Fields.RANK, 10 + gridrank);
      }
    }
    return items;
  }

  /** Ordering defines the precedence of place classes. */
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

    private static final Map<String, PlaceType> byName = new HashMap<>();

    static {
      for (PlaceType place : values()) {
        byName.put(place.name, place);
      }
    }

    private final String name;

    PlaceType(String name) {
      this.name = name;
    }

    public static PlaceType forName(String name) {
      return byName.getOrDefault(name, UNKNOWN);
    }
  }

  /**
   * Information extracted from a natural earth geographic region that will be inspected when joining with OpenStreetMap
   * data.
   */
  private static record NaturalEarthRegion(String name, int rank) {

    NaturalEarthRegion(String name, int maxRank, double... ranks) {
      this(name, (int) Math.ceil(DoubleStream.of(ranks).average().orElse(maxRank)));
    }
  }

  /**
   * Information extracted from a natural earth place label that will be inspected when joining with OpenStreetMap
   * data.
   */
  private static record NaturalEarthPoint(String name, String wikidata, int scaleRank, Set<String> names) {}
}

