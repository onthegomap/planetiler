/*
Copyright (c) 2021, MapTiler.com & OpenMapTiles contributors.
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

import static com.onthegomap.planetiler.basemap.util.Utils.*;
import static com.onthegomap.planetiler.util.MemoryEstimator.CLASS_HEADER_BYTES;
import static com.onthegomap.planetiler.util.MemoryEstimator.POINTER_BYTES;
import static com.onthegomap.planetiler.util.MemoryEstimator.estimateSize;
import static java.util.Map.entry;

import com.onthegomap.planetiler.FeatureCollector;
import com.onthegomap.planetiler.FeatureMerge;
import com.onthegomap.planetiler.VectorTile;
import com.onthegomap.planetiler.basemap.BasemapProfile;
import com.onthegomap.planetiler.basemap.generated.OpenMapTilesSchema;
import com.onthegomap.planetiler.basemap.generated.Tables;
import com.onthegomap.planetiler.config.PlanetilerConfig;
import com.onthegomap.planetiler.expression.MultiExpression;
import com.onthegomap.planetiler.geo.GeoUtils;
import com.onthegomap.planetiler.geo.GeometryException;
import com.onthegomap.planetiler.reader.SourceFeature;
import com.onthegomap.planetiler.reader.osm.OsmElement;
import com.onthegomap.planetiler.reader.osm.OsmReader;
import com.onthegomap.planetiler.reader.osm.OsmRelationInfo;
import com.onthegomap.planetiler.stats.Stats;
import com.onthegomap.planetiler.util.MemoryEstimator;
import com.onthegomap.planetiler.util.Parse;
import com.onthegomap.planetiler.util.Translations;
import com.onthegomap.planetiler.util.ZoomFunction;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.prep.PreparedGeometry;
import org.locationtech.jts.geom.prep.PreparedGeometryFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Defines the logic for generating map elements for roads, shipways, railroads, and paths in the {@code transportation}
 * layer from source features.
 * <p>
 * This class is ported to Java from <a href="https://github.com/openmaptiles/openmaptiles/tree/master/layers/transportation">OpenMapTiles
 * transportation sql files</a>.
 */
public class Transportation implements
  OpenMapTilesSchema.Transportation,
  Tables.OsmAerialwayLinestring.Handler,
  Tables.OsmHighwayLinestring.Handler,
  Tables.OsmRailwayLinestring.Handler,
  Tables.OsmShipwayLinestring.Handler,
  Tables.OsmHighwayPolygon.Handler,
  BasemapProfile.NaturalEarthProcessor,
  BasemapProfile.FeaturePostProcessor,
  BasemapProfile.OsmRelationPreprocessor,
  BasemapProfile.IgnoreWikidata {

  /*
   * Generates the shape for roads, trails, ferries, railways with detailed
   * attributes for rendering, but not any names.  The transportation_name
   * layer includes names, but less detailed attributes.
   */

  private static final Logger LOGGER = LoggerFactory.getLogger(Transportation.class);
  private static final Pattern GREAT_BRITAIN_REF_NETWORK_PATTERN = Pattern.compile("^[AM][0-9AM()]+");
  private static final MultiExpression.Index<String> classMapping = FieldMappings.Class.index();
  private static final Set<String> RAILWAY_RAIL_VALUES = Set.of(
    FieldValues.SUBCLASS_RAIL,
    FieldValues.SUBCLASS_NARROW_GAUGE,
    FieldValues.SUBCLASS_PRESERVED,
    FieldValues.SUBCLASS_FUNICULAR
  );
  private static final Set<String> RAILWAY_TRANSIT_VALUES = Set.of(
    FieldValues.SUBCLASS_SUBWAY,
    FieldValues.SUBCLASS_LIGHT_RAIL,
    FieldValues.SUBCLASS_MONORAIL,
    FieldValues.SUBCLASS_TRAM
  );
  private static final Set<String> SERVICE_VALUES = Set.of(
    FieldValues.SERVICE_SPUR,
    FieldValues.SERVICE_YARD,
    FieldValues.SERVICE_SIDING,
    FieldValues.SERVICE_CROSSOVER,
    FieldValues.SERVICE_DRIVEWAY,
    FieldValues.SERVICE_ALLEY,
    FieldValues.SERVICE_PARKING_AISLE
  );
  private static final Set<String> SURFACE_UNPAVED_VALUES = Set.of(
    "unpaved", "compacted", "dirt", "earth", "fine_gravel", "grass", "grass_paver", "gravel", "gravel_turf", "ground",
    "ice", "mud", "pebblestone", "salt", "sand", "snow", "woodchips"
  );
  private static final Set<String> SURFACE_PAVED_VALUES = Set.of(
    "paved", "asphalt", "cobblestone", "concrete", "concrete:lanes", "concrete:plates", "metal",
    "paving_stones", "sett", "unhewn_cobblestone", "wood"
  );
  private static final Set<String> ACCESS_NO_VALUES = Set.of(
    "private", "no"
  );
  private static final ZoomFunction.MeterToPixelThresholds MIN_LENGTH = ZoomFunction.meterThresholds()
    .put(7, 50)
    .put(6, 100)
    .put(5, 500)
    .put(4, 1_000);
  // ORDER BY network_type, network, LENGTH(ref), ref)
  private static final Comparator<RouteRelation> RELATION_ORDERING = Comparator
    .<RouteRelation>comparingInt(
      r -> r.networkType() != null ? r.networkType.ordinal() : Integer.MAX_VALUE)
    .thenComparing(routeRelation -> coalesce(routeRelation.network(), ""))
    .thenComparingInt(r -> r.ref().length())
    .thenComparing(RouteRelation::ref);
  private final AtomicBoolean loggedNoGb = new AtomicBoolean(false);
  private final boolean z13Paths;
  private PreparedGeometry greatBritain = null;
  private final Map<String, Integer> MINZOOMS;
  private final Stats stats;
  private final PlanetilerConfig config;

  public Transportation(Translations translations, PlanetilerConfig config, Stats stats) {
    this.config = config;
    this.stats = stats;
    z13Paths = config.arguments().getBoolean(
      "transportation_z13_paths",
      "transportation(_name) layer: show all paths on z13",
      false
    );
    MINZOOMS = Map.ofEntries(
      entry(FieldValues.CLASS_PATH, z13Paths ? 13 : 14),
      entry(FieldValues.CLASS_TRACK, 14),
      entry(FieldValues.CLASS_SERVICE, 13),
      entry(FieldValues.CLASS_MINOR, 13),
      entry(FieldValues.CLASS_RACEWAY, 12),
      entry(FieldValues.CLASS_TERTIARY, 11),
      entry(FieldValues.CLASS_BUSWAY, 11),
      entry(FieldValues.CLASS_SECONDARY, 9),
      entry(FieldValues.CLASS_PRIMARY, 7),
      entry(FieldValues.CLASS_TRUNK, 5),
      entry(FieldValues.CLASS_MOTORWAY, 4)
    );
  }

  @Override
  public void processNaturalEarth(String table, SourceFeature feature,
    FeatureCollector features) {
    if ("ne_10m_admin_0_countries".equals(table) && feature.hasTag("iso_a2", "GB")) {
      // multiple threads call this method concurrently, GB polygon *should* only be found
      // once, but just to be safe synchronize updates to that field
      synchronized (this) {
        try {
          Geometry boundary = feature.polygon().buffer(GeoUtils.metersToPixelAtEquator(0, 10_000) / 256d);
          greatBritain = PreparedGeometryFactory.prepare(boundary);
        } catch (GeometryException e) {
          LOGGER.error("Failed to get Great Britain Polygon: " + e);
        }
      }
    }
  }

  /** Returns a value for {@code surface} tag constrained to a small set of known values from raw OSM data. */
  private static String surface(String value) {
    return value == null ? null : SURFACE_PAVED_VALUES.contains(value) ? FieldValues.SURFACE_PAVED :
      SURFACE_UNPAVED_VALUES.contains(value) ? FieldValues.SURFACE_UNPAVED : null;
  }

  /** Returns a value for {@code access} tag constrained to a small set of known values from raw OSM data. */
  private static String access(String value) {
    return value == null ? null : ACCESS_NO_VALUES.contains(value) ? "no" : null;
  }

  /** Returns a value for {@code service} tag constrained to a small set of known values from raw OSM data. */
  private static String service(String value) {
    return (value == null || !SERVICE_VALUES.contains(value)) ? null : value;
  }

  private static String railwayClass(String value) {
    return value == null ? null :
      RAILWAY_RAIL_VALUES.contains(value) ? "rail" :
        RAILWAY_TRANSIT_VALUES.contains(value) ? "transit" : null;
  }

  static String highwayClass(String highway, String publicTransport, String construction, String manMade) {
    return (!nullOrEmpty(highway) || !nullOrEmpty(publicTransport)) ? classMapping.getOrElse(Map.of(
      "highway", coalesce(highway, ""),
      "public_transport", coalesce(publicTransport, ""),
      "construction", coalesce(construction, "")
    ), null) : manMade;
  }

  static String highwaySubclass(String highwayClass, String publicTransport, String highway) {
    return FieldValues.CLASS_PATH.equals(highwayClass) ? coalesce(nullIfEmpty(publicTransport), highway) : null;
  }

  static boolean isFootwayOrSteps(String highway) {
    return "footway".equals(highway) || "steps".equals(highway);
  }

  static boolean isLink(String highway) {
    return highway != null && highway.endsWith("_link");
  }

  private static boolean isResidentialOrUnclassified(String highway) {
    return "residential".equals(highway) || "unclassified".equals(highway);
  }

  private static boolean isDrivewayOrParkingAisle(String service) {
    return FieldValues.SERVICE_PARKING_AISLE.equals(service) || FieldValues.SERVICE_DRIVEWAY.equals(service);
  }

  private static boolean isBridgeOrPier(String manMade) {
    return "bridge".equals(manMade) || "pier".equals(manMade);
  }

  enum RouteNetwork {

    US_INTERSTATE("us-interstate"),
    US_HIGHWAY("us-highway"),
    US_STATE("us-state"),
    CA_TRANSCANADA("ca-transcanada"),
    GB_MOTORWAY("gb-motorway"),
    GB_TRUNK("gb-trunk");

    final String name;

    RouteNetwork(String name) {
      this.name = name;
    }
  }

  @Override
  public List<OsmRelationInfo> preprocessOsmRelation(OsmElement.Relation relation) {
    if (relation.hasTag("route", "road", "hiking")) {
      RouteNetwork networkType = null;
      String network = relation.getString("network");
      String ref = relation.getString("ref");

      if ("US:I".equals(network)) {
        networkType = RouteNetwork.US_INTERSTATE;
      } else if ("US:US".equals(network)) {
        networkType = RouteNetwork.US_HIGHWAY;
      } else if (network != null && network.length() == 5 && network.startsWith("US:")) {
        networkType = RouteNetwork.US_STATE;
      } else if (network != null && network.startsWith("CA:transcanada")) {
        networkType = RouteNetwork.CA_TRANSCANADA;
      }

      int rank = switch (coalesce(network, "")) {
        case "iwn", "nwn", "rwn" -> 1;
        case "lwn" -> 2;
        default -> (relation.hasTag("osmc:symbol") || relation.hasTag("colour")) ? 2 : 3;
      };

      if (network != null || rank < 3) {
        return List.of(new RouteRelation(coalesce(ref, ""), network, networkType, (byte) rank, relation.id()));
      }
    }
    return null;
  }

  List<RouteRelation> getRouteRelations(Tables.OsmHighwayLinestring element) {
    String ref = element.ref();
    List<OsmReader.RelationMember<RouteRelation>> relations = element.source().relationInfo(RouteRelation.class);
    List<RouteRelation> result = new ArrayList<>(relations.size() + 1);
    for (var relationMember : relations) {
      var relation = relationMember.relation();
      // avoid duplicates - list should be very small and usually only one
      if (!result.contains(relation)) {
        result.add(relation);
      }
    }
    if (ref != null) {
      // GB doesn't use regular relations like everywhere else, so if we are
      // in GB then use a naming convention instead.
      Matcher refMatcher = GREAT_BRITAIN_REF_NETWORK_PATTERN.matcher(ref);
      if (refMatcher.find()) {
        if (greatBritain == null) {
          if (!loggedNoGb.get() && loggedNoGb.compareAndSet(false, true)) {
            LOGGER.warn("No GB polygon for inferring route network types");
          }
        } else {
          try {
            Geometry wayGeometry = element.source().worldGeometry();
            if (greatBritain.intersects(wayGeometry)) {
              Transportation.RouteNetwork networkType =
                "motorway".equals(element.highway()) ? Transportation.RouteNetwork.GB_MOTORWAY
                  : Transportation.RouteNetwork.GB_TRUNK;
              String network = "motorway".equals(element.highway()) ? "omt-gb-motorway" : "omt-gb-trunk";
              result.add(new RouteRelation(refMatcher.group(), network, networkType, (byte) -1,
                0));
            }
          } catch (GeometryException e) {
            e.log(stats, "omt_transportation_name_gb_test",
              "Unable to test highway against GB route network: " + element.source().id());
          }
        }
      }
    }
    Collections.sort(result);
    return result;
  }

  RouteRelation getRouteRelation(Tables.OsmHighwayLinestring element) {
    List<RouteRelation> all = getRouteRelations(element);
    return all.isEmpty() ? null : all.get(0);
  }

  @Override
  public void process(Tables.OsmHighwayLinestring element, FeatureCollector features) {
    if (element.isArea()) {
      return;
    }

    RouteRelation routeRelation = getRouteRelation(element);
    RouteNetwork networkType = routeRelation != null ? routeRelation.networkType : null;

    String highway = element.highway();
    String highwayClass = highwayClass(element.highway(), element.publicTransport(), element.construction(),
      element.manMade());
    String service = service(element.service());
    if (highwayClass != null) {
      if (isPierPolygon(element)) {
        return;
      }
      int minzoom = getMinzoom(element, highwayClass);

      boolean highwayRamp = isLink(highway);
      Integer rampAboveZ12 = (highwayRamp || element.isRamp()) ? 1 : null;
      Integer rampBelowZ12 = highwayRamp ? 1 : null;

      FeatureCollector.Feature feature = features.line(LAYER_NAME).setBufferPixels(BUFFER_SIZE)
        // main attributes at all zoom levels (used for grouping <= z8)
        .setAttr(Fields.CLASS, highwayClass)
        .setAttr(Fields.SUBCLASS, highwaySubclass(highwayClass, element.publicTransport(), highway))
        .setAttr(Fields.BRUNNEL, brunnel(element.isBridge(), element.isTunnel(), element.isFord()))
        .setAttr(Fields.NETWORK, networkType != null ? networkType.name : null)
        // z8+
        .setAttrWithMinzoom(Fields.EXPRESSWAY, element.expressway() && !"motorway".equals(highway) ? 1 : null, 8)
        // z9+
        .setAttrWithMinzoom(Fields.LAYER, nullIfLong(element.layer(), 0), 9)
        .setAttrWithMinzoom(Fields.BICYCLE, nullIfEmpty(element.bicycle()), 9)
        .setAttrWithMinzoom(Fields.FOOT, nullIfEmpty(element.foot()), 9)
        .setAttrWithMinzoom(Fields.HORSE, nullIfEmpty(element.horse()), 9)
        .setAttrWithMinzoom(Fields.MTB_SCALE, nullIfEmpty(element.mtbScale()), 9)
        .setAttrWithMinzoom(Fields.ACCESS, access(element.access()), 9)
        .setAttrWithMinzoom(Fields.TOLL, element.toll() ? 1 : null, 9)
        // sometimes z9+, sometimes z12+
        .setAttr(Fields.RAMP, minzoom >= 12 ? rampAboveZ12 :
          ((ZoomFunction<Integer>) z -> z < 9 ? null : z >= 12 ? rampAboveZ12 : rampBelowZ12))
        // z12+
        .setAttrWithMinzoom(Fields.SERVICE, service, 12)
        .setAttrWithMinzoom(Fields.ONEWAY, nullIfInt(element.isOneway(), 0), 12)
        .setAttrWithMinzoom(Fields.SURFACE, surface(element.surface()), 12)
        .setMinPixelSize(0) // merge during post-processing, then limit by size
        .setSortKey(element.zOrder())
        .setMinZoom(minzoom);

      if (isFootwayOrSteps(highway)) {
        feature
          .setAttr(Fields.LEVEL, Parse.parseLongOrNull(element.source().getTag("level")))
          .setAttr(Fields.INDOOR, element.indoor() ? 1 : null);
      }
    }
  }

  int getMinzoom(Tables.OsmHighwayLinestring element, String highwayClass) {
    List<RouteRelation> routeRelations = getRouteRelations(element);
    int routeRank = 3;
    for (var rel : routeRelations) {
      if (rel.intRank() < routeRank) {
        routeRank = rel.intRank();
      }
    }
    String highway = element.highway();

    int minzoom;
    if ("pier".equals(element.manMade())) {
      minzoom = 13;
    } else if (isResidentialOrUnclassified(highway)) {
      minzoom = 12;
    } else {
      String baseClass = highwayClass.replace("_construction", "");
      minzoom = switch (baseClass) {
        case FieldValues.CLASS_SERVICE -> isDrivewayOrParkingAisle(service(element.service())) ? 14 : 13;
        case FieldValues.CLASS_TRACK, FieldValues.CLASS_PATH -> routeRank == 1 ? 12 :
          (z13Paths || !nullOrEmpty(element.name()) || routeRank <= 2 || !nullOrEmpty(element.sacScale())) ? 13 : 14;
        default -> MINZOOMS.get(baseClass);
      };
    }

    if (isLink(highway)) {
      minzoom = Math.max(minzoom, 9);
    }
    return minzoom;
  }

  private boolean isPierPolygon(Tables.OsmHighwayLinestring element) {
    if ("pier".equals(element.manMade())) {
      try {
        if (element.source().worldGeometry() instanceof LineString lineString && lineString.isClosed()) {
          // ignore this because it's a polygon
          return true;
        }
      } catch (GeometryException e) {
        e.log(stats, "omt_transportation_pier",
          "Unable to decode pier geometry for " + element.source().id());
        return true;
      }
    }
    return false;
  }

  @Override
  public void process(Tables.OsmRailwayLinestring element, FeatureCollector features) {
    String railway = element.railway();
    String clazz = railwayClass(railway);
    if (clazz != null) {
      String service = nullIfEmpty(element.service());
      int minzoom;
      if (service != null) {
        minzoom = 14;
      } else if (FieldValues.SUBCLASS_RAIL.equals(railway)) {
        minzoom = "main".equals(element.usage()) ? 8 : 10;
      } else if (FieldValues.SUBCLASS_NARROW_GAUGE.equals(railway)) {
        minzoom = 10;
      } else if (FieldValues.SUBCLASS_LIGHT_RAIL.equals(railway)) {
        minzoom = 11;
      } else {
        minzoom = 14;
      }
      features.line(LAYER_NAME).setBufferPixels(BUFFER_SIZE)
        .setAttr(Fields.CLASS, clazz)
        .setAttr(Fields.SUBCLASS, railway)
        .setAttr(Fields.SERVICE, service(service))
        .setAttr(Fields.ONEWAY, nullIfInt(element.isOneway(), 0))
        .setAttr(Fields.RAMP, element.isRamp() ? 1L : null)
        .setAttrWithMinzoom(Fields.BRUNNEL, brunnel(element.isBridge(), element.isTunnel(), element.isFord()), 10)
        .setAttrWithMinzoom(Fields.LAYER, nullIfLong(element.layer(), 0), 9)
        .setSortKey(element.zOrder())
        .setMinPixelSize(0) // merge during post-processing, then limit by size
        .setMinZoom(minzoom);
    }
  }

  @Override
  public void process(Tables.OsmAerialwayLinestring element, FeatureCollector features) {
    features.line(LAYER_NAME).setBufferPixels(BUFFER_SIZE)
      .setAttr(Fields.CLASS, "aerialway")
      .setAttr(Fields.SUBCLASS, element.aerialway())
      .setAttr(Fields.SERVICE, service(element.service()))
      .setAttr(Fields.ONEWAY, nullIfInt(element.isOneway(), 0))
      .setAttr(Fields.RAMP, element.isRamp() ? 1L : null)
      .setAttr(Fields.BRUNNEL, brunnel(element.isBridge(), element.isTunnel(), element.isFord()))
      .setAttr(Fields.LAYER, nullIfLong(element.layer(), 0))
      .setSortKey(element.zOrder())
      .setMinPixelSize(0) // merge during post-processing, then limit by size
      .setMinZoom(12);
  }

  @Override
  public void process(Tables.OsmShipwayLinestring element, FeatureCollector features) {
    features.line(LAYER_NAME).setBufferPixels(BUFFER_SIZE)
      .setAttr(Fields.CLASS, element.shipway()) // "ferry"
      // no subclass
      .setAttr(Fields.SERVICE, service(element.service()))
      .setAttr(Fields.ONEWAY, nullIfInt(element.isOneway(), 0))
      .setAttr(Fields.RAMP, element.isRamp() ? 1L : null)
      .setAttr(Fields.BRUNNEL, brunnel(element.isBridge(), element.isTunnel(), element.isFord()))
      .setAttr(Fields.LAYER, nullIfLong(element.layer(), 0))
      .setSortKey(element.zOrder())
      .setMinPixelSize(0) // merge during post-processing, then limit by size
      .setMinZoom(11);
  }

  @Override
  public void process(Tables.OsmHighwayPolygon element, FeatureCollector features) {
    String manMade = element.manMade();
    if (isBridgeOrPier(manMade) ||
      // only allow closed ways where area=yes, and multipolygons
      // and ignore underground pedestrian areas
      (!element.source().canBeLine() && element.layer() >= 0)) {
      String highwayClass = highwayClass(element.highway(), element.publicTransport(), null, element.manMade());
      if (highwayClass != null) {
        features.polygon(LAYER_NAME).setBufferPixels(BUFFER_SIZE)
          .setAttr(Fields.CLASS, highwayClass)
          .setAttr(Fields.SUBCLASS, highwaySubclass(highwayClass, element.publicTransport(), element.highway()))
          .setAttr(Fields.BRUNNEL, brunnel("bridge".equals(manMade), false, false))
          .setAttr(Fields.LAYER, nullIfLong(element.layer(), 0))
          .setSortKey(element.zOrder())
          .setMinZoom(13);
      }
    }
  }

  @Override
  public List<VectorTile.Feature> postProcess(int zoom, List<VectorTile.Feature> items) {
    double tolerance = config.tolerance(zoom);
    double minLength = coalesce(MIN_LENGTH.apply(zoom), 0).doubleValue();
    // TODO preserve direction for one-way?
    return FeatureMerge.mergeLineStrings(items, minLength, tolerance, BUFFER_SIZE);
  }

  /** Information extracted from route relations to use when processing ways in that relation. */
  record RouteRelation(
    String ref,
    String network,
    RouteNetwork networkType,
    byte rank,
    @Override long id
  ) implements OsmRelationInfo, Comparable<RouteRelation> {

    @Override
    public long estimateMemoryUsageBytes() {
      return CLASS_HEADER_BYTES +
        MemoryEstimator.estimateSize(rank) +
        POINTER_BYTES + estimateSize(ref) +
        POINTER_BYTES + estimateSize(network) +
        POINTER_BYTES + // networkType
        MemoryEstimator.estimateSizeLong(id);
    }

    public int intRank() {
      return rank;
    }

    @Override
    public int compareTo(RouteRelation o) {
      return RELATION_ORDERING.compare(this, o);
    }
  }
}
