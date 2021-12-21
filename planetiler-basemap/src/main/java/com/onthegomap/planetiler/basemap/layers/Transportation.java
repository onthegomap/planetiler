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

import static com.onthegomap.planetiler.basemap.util.Utils.*;

import com.onthegomap.planetiler.FeatureCollector;
import com.onthegomap.planetiler.FeatureMerge;
import com.onthegomap.planetiler.VectorTile;
import com.onthegomap.planetiler.basemap.BasemapProfile;
import com.onthegomap.planetiler.basemap.generated.OpenMapTilesSchema;
import com.onthegomap.planetiler.basemap.generated.Tables;
import com.onthegomap.planetiler.config.PlanetilerConfig;
import com.onthegomap.planetiler.expression.MultiExpression;
import com.onthegomap.planetiler.geo.GeometryException;
import com.onthegomap.planetiler.stats.Stats;
import com.onthegomap.planetiler.util.Parse;
import com.onthegomap.planetiler.util.Translations;
import com.onthegomap.planetiler.util.ZoomFunction;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.locationtech.jts.geom.LineString;

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
  BasemapProfile.FeaturePostProcessor,
  BasemapProfile.IgnoreWikidata {

  /*
   * Generates the shape for roads, trails, ferries, railways with detailed
   * attributes for rendering, but not any names.  The transportation_name
   * layer includes names, but less detailed attributes.
   */

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
  private static final ZoomFunction.MeterToPixelThresholds MIN_LENGTH = ZoomFunction.meterThresholds()
    .put(7, 50)
    .put(6, 100)
    .put(5, 500)
    .put(4, 1_000);
  private final Map<String, Integer> MINZOOMS;
  private final Stats stats;
  private final PlanetilerConfig config;

  public Transportation(Translations translations, PlanetilerConfig config, Stats stats) {
    this.config = config;
    this.stats = stats;
    boolean z13Paths = config.arguments().getBoolean(
      "transportation_z13_paths",
      "transportation(_name) layer: show paths on z13",
      true
    );
    MINZOOMS = Map.of(
      FieldValues.CLASS_TRACK, 14,
      FieldValues.CLASS_PATH, z13Paths ? 13 : 14,
      FieldValues.CLASS_MINOR, 13,
      FieldValues.CLASS_RACEWAY, 12,
      FieldValues.CLASS_TERTIARY, 11,
      FieldValues.CLASS_SECONDARY, 9,
      FieldValues.CLASS_PRIMARY, 7,
      FieldValues.CLASS_TRUNK, 5,
      FieldValues.CLASS_MOTORWAY, 4
    );
  }

  /** Returns a value for {@code surface} tag constrained to a small set of known values from raw OSM data. */
  private static String surface(String value) {
    return value == null ? null : SURFACE_PAVED_VALUES.contains(value) ? FieldValues.SURFACE_PAVED :
      SURFACE_UNPAVED_VALUES.contains(value) ? FieldValues.SURFACE_UNPAVED : null;
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
    ), manMade) : manMade;
  }

  static String highwaySubclass(String highwayClass, String publicTransport, String highway) {
    return FieldValues.CLASS_PATH.equals(highwayClass) ? coalesce(nullIfEmpty(publicTransport), highway) : null;
  }

  static boolean isFootwayOrSteps(String highway) {
    return "footway".equals(highway) || "steps".equals(highway);
  }

  private static boolean isResidentialOrUnclassified(String highway) {
    return "residential".equals(highway) || "unclassified".equals(highway);
  }

  private static boolean isBridgeOrPier(String manMade) {
    return "bridge".equals(manMade) || "pier".equals(manMade);
  }

  @Override
  public void process(Tables.OsmHighwayLinestring element, FeatureCollector features) {
    if (element.isArea()) {
      return;
    }

    String highway = element.highway();
    String highwayClass = highwayClass(element.highway(), element.publicTransport(), element.construction(),
      element.manMade());
    if (highwayClass != null) {
      int minzoom;
      if ("pier".equals(element.manMade())) {
        try {
          if (element.source().worldGeometry() instanceof LineString lineString && lineString.isClosed()) {
            // ignore this because it's a polygon
            return;
          }
        } catch (GeometryException e) {
          e.log(stats, "omt_transportation_pier",
            "Unable to decode pier geometry for " + element.source().id());
          return;
        }
        minzoom = 13;
      } else if (isResidentialOrUnclassified(highway)) {
        minzoom = 12;
      } else {
        String baseClass = highwayClass.replace("_construction", "");
        minzoom = MINZOOMS.getOrDefault(baseClass, 12);
      }
      boolean highwayIsLink = coalesce(highway, "").endsWith("_link");

      if (highwayIsLink) {
        minzoom = Math.max(minzoom, 9);
      }

      boolean highwayRamp = highwayIsLink || "steps".equals(highway);
      int rampAboveZ12 = (highwayRamp || element.isRamp()) ? 1 : 0;
      int rampBelowZ12 = highwayRamp ? 1 : 0;

      FeatureCollector.Feature feature = features.line(LAYER_NAME).setBufferPixels(BUFFER_SIZE)
        // main attributes at all zoom levels (used for grouping <= z8)
        .setAttr(Fields.CLASS, highwayClass)
        .setAttr(Fields.SUBCLASS, highwaySubclass(highwayClass, element.publicTransport(), highway))
        .setAttr(Fields.BRUNNEL, brunnel(element.isBridge(), element.isTunnel(), element.isFord()))
        // rest at z9+
        .setAttrWithMinzoom(Fields.SERVICE, service(element.service()), 12)
        .setAttrWithMinzoom(Fields.ONEWAY, element.isOneway(), 12)
        .setAttr(Fields.RAMP, minzoom >= 12 ? rampAboveZ12 :
          ((ZoomFunction<Integer>) z -> z < 9 ? null : z >= 12 ? rampAboveZ12 : rampBelowZ12))
        .setAttrWithMinzoom(Fields.LAYER, nullIf(element.layer(), 0), 9)
        .setAttrWithMinzoom(Fields.BICYCLE, nullIfEmpty(element.bicycle()), 9)
        .setAttrWithMinzoom(Fields.FOOT, nullIfEmpty(element.foot()), 9)
        .setAttrWithMinzoom(Fields.HORSE, nullIfEmpty(element.horse()), 9)
        .setAttrWithMinzoom(Fields.MTB_SCALE, nullIfEmpty(element.mtbScale()), 9)
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
        .setAttr(Fields.ONEWAY, element.isOneway())
        .setAttr(Fields.RAMP, element.isRamp() ? 1 : 0)
        .setAttrWithMinzoom(Fields.BRUNNEL, brunnel(element.isBridge(), element.isTunnel(), element.isFord()), 10)
        .setAttrWithMinzoom(Fields.LAYER, nullIf(element.layer(), 0), 9)
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
      .setAttr(Fields.ONEWAY, element.isOneway())
      .setAttr(Fields.RAMP, element.isRamp() ? 1 : 0)
      .setAttr(Fields.BRUNNEL, brunnel(element.isBridge(), element.isTunnel(), element.isFord()))
      .setAttr(Fields.LAYER, nullIf(element.layer(), 0))
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
      .setAttr(Fields.ONEWAY, element.isOneway())
      .setAttr(Fields.RAMP, element.isRamp() ? 1 : 0)
      .setAttr(Fields.BRUNNEL, brunnel(element.isBridge(), element.isTunnel(), element.isFord()))
      .setAttr(Fields.LAYER, nullIf(element.layer(), 0))
      .setSortKey(element.zOrder())
      .setMinPixelSize(0) // merge during post-processing, then limit by size
      .setMinZoom(11);
  }

  @Override
  public void process(Tables.OsmHighwayPolygon element, FeatureCollector features) {
    String manMade = element.manMade();
    if (isBridgeOrPier(manMade) ||
      // ignore underground pedestrian areas
      (element.isArea() && element.layer() >= 0)) {
      String highwayClass = highwayClass(element.highway(), element.publicTransport(), null, element.manMade());
      if (highwayClass != null) {
        features.polygon(LAYER_NAME).setBufferPixels(BUFFER_SIZE)
          .setAttr(Fields.CLASS, highwayClass)
          .setAttr(Fields.SUBCLASS, highwaySubclass(highwayClass, element.publicTransport(), element.highway()))
          .setAttr(Fields.BRUNNEL, brunnel("bridge".equals(manMade), false, false))
          .setAttr(Fields.LAYER, nullIf(element.layer(), 0))
          .setSortKey(element.zOrder())
          .setMinZoom(13);
      }
    }
  }

  @Override
  public List<VectorTile.Feature> postProcess(int zoom, List<VectorTile.Feature> items) {
    double tolerance = config.tolerance(zoom);
    double minLength = coalesce(MIN_LENGTH.apply(zoom), config.minFeatureSize(zoom)).doubleValue();
    return FeatureMerge.mergeLineStrings(items, minLength, tolerance, BUFFER_SIZE);
  }
}
