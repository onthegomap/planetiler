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

import static com.onthegomap.planetiler.basemap.layers.Transportation.highwayClass;
import static com.onthegomap.planetiler.basemap.layers.Transportation.highwaySubclass;
import static com.onthegomap.planetiler.basemap.layers.Transportation.isFootwayOrSteps;
import static com.onthegomap.planetiler.basemap.util.Utils.*;

import com.carrotsearch.hppc.LongArrayList;
import com.carrotsearch.hppc.LongByteMap;
import com.onthegomap.planetiler.FeatureCollector;
import com.onthegomap.planetiler.FeatureMerge;
import com.onthegomap.planetiler.ForwardingProfile;
import com.onthegomap.planetiler.VectorTile;
import com.onthegomap.planetiler.basemap.BasemapProfile;
import com.onthegomap.planetiler.basemap.generated.OpenMapTilesSchema;
import com.onthegomap.planetiler.basemap.generated.Tables;
import com.onthegomap.planetiler.basemap.util.LanguageUtils;
import com.onthegomap.planetiler.collection.Hppc;
import com.onthegomap.planetiler.config.PlanetilerConfig;
import com.onthegomap.planetiler.reader.osm.OsmElement;
import com.onthegomap.planetiler.stats.Stats;
import com.onthegomap.planetiler.util.Parse;
import com.onthegomap.planetiler.util.Translations;
import com.onthegomap.planetiler.util.ZoomFunction;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * Defines the logic for generating map elements for road, shipway, rail, and path names in the {@code
 * transportation_name} layer from source features.
 * <p>
 * This class is ported to Java from
 * <a href="https://github.com/openmaptiles/openmaptiles/tree/master/layers/transportation_name">OpenMapTiles
 * transportation_name sql files</a>.
 */
public class TransportationName implements
  OpenMapTilesSchema.TransportationName,
  Tables.OsmHighwayPoint.Handler,
  Tables.OsmHighwayLinestring.Handler,
  Tables.OsmAerialwayLinestring.Handler,
  Tables.OsmShipwayLinestring.Handler,
  BasemapProfile.FeaturePostProcessor,
  BasemapProfile.IgnoreWikidata,
  ForwardingProfile.OsmNodePreprocessor,
  ForwardingProfile.OsmWayPreprocessor {

  /*
   * Generate road names from OSM data.  Route networkType and ref are copied
   * from relations that roads are a part of - except in Great Britain which
   * uses a naming convention instead of relations.
   *
   * The goal is to make name linestrings as long as possible to give clients
   * the best chance of showing road names at different zoom levels, so do not
   * limit linestrings by length at process time and merge them at tile
   * render-time.
   *
   * Any 3-way nodes and intersections break line merging so set the
   * transportation_name_limit_merge argument to true to add temporary
   * "is link" and "relation" keys to prevent opposite directions of a
   * divided highway or on/off ramps from getting merged for main highways.
   */

  // extra temp key used to group on/off-ramps separately from main highways
  private static final String LINK_TEMP_KEY = "__islink";
  private static final String RELATION_ID_TEMP_KEY = "__relid";

  private static final ZoomFunction.MeterToPixelThresholds MIN_LENGTH = ZoomFunction.meterThresholds()
    .put(6, 20_000)
    .put(7, 20_000)
    .put(8, 14_000)
    .put(9, 8_000)
    .put(10, 8_000)
    .put(11, 8_000);
  private static final List<String> CONCURRENT_ROUTE_KEYS = List.of(
    Fields.ROUTE_1,
    Fields.ROUTE_2,
    Fields.ROUTE_3,
    Fields.ROUTE_4,
    Fields.ROUTE_5,
    Fields.ROUTE_6
  );
  private final boolean brunnel;
  private final boolean sizeForShield;
  private final boolean limitMerge;
  private final PlanetilerConfig config;
  private Transportation transportation;
  private final LongByteMap motorwayJunctionHighwayClasses = Hppc.newLongByteHashMap();

  public TransportationName(Translations translations, PlanetilerConfig config, Stats stats) {
    this.config = config;
    this.brunnel = config.arguments().getBoolean(
      "transportation_name_brunnel",
      "transportation_name layer: set to false to omit brunnel and help merge long highways",
      false
    );
    this.sizeForShield = config.arguments().getBoolean(
      "transportation_name_size_for_shield",
      "transportation_name layer: allow road names on shorter segments (ie. they will have a shield)",
      false
    );
    this.limitMerge = config.arguments().getBoolean(
      "transportation_name_limit_merge",
      "transportation_name layer: limit merge so we don't combine different relations to help merge long highways",
      false
    );
  }

  public void needsTransportationLayer(Transportation transportation) {
    this.transportation = transportation;
  }


  @Override
  public void preprocessOsmNode(OsmElement.Node node) {
    if (node.hasTag("highway", "motorway_junction")) {
      synchronized (motorwayJunctionHighwayClasses) {
        motorwayJunctionHighwayClasses.put(node.id(), HighwayClass.UNKNOWN.value);
      }
    }
  }

  @Override
  public void preprocessOsmWay(OsmElement.Way way) {
    String highway = way.getString("highway");
    if (highway != null) {
      HighwayClass cls = HighwayClass.from(highway);
      if (cls != HighwayClass.UNKNOWN) {
        LongArrayList nodes = way.nodes();
        synchronized (motorwayJunctionHighwayClasses) {
          for (int i = 0; i < nodes.size(); i++) {
            long node = nodes.get(i);
            if (motorwayJunctionHighwayClasses.containsKey(node)) {
              byte oldValue = motorwayJunctionHighwayClasses.get(node);
              byte newValue = cls.value;
              if (newValue > oldValue) {
                motorwayJunctionHighwayClasses.put(node, newValue);
              }
            }
          }
        }
      }
    }
  }

  @Override
  public void process(Tables.OsmHighwayPoint element, FeatureCollector features) {
    long id = element.source().id();
    byte value = motorwayJunctionHighwayClasses.getOrDefault(id, (byte) -1);
    if (value > 0) {
      HighwayClass cls = HighwayClass.from(value);
      if (cls != HighwayClass.UNKNOWN) {
        String subclass = FieldValues.SUBCLASS_JUNCTION;
        String ref = element.ref();

        features.point(LAYER_NAME)
          .setBufferPixels(BUFFER_SIZE)
          .putAttrs(LanguageUtils.getNamesWithoutTranslations(element.source().tags()))
          .setAttr(Fields.REF, ref)
          .setAttr(Fields.REF_LENGTH, ref != null ? ref.length() : null)
          .setAttr(Fields.CLASS, highwayClass(cls.highwayValue, null, null, null))
          .setAttr(Fields.SUBCLASS, subclass)
          .setAttr(Fields.LAYER, nullIfLong(element.layer(), 0))
          .setSortKeyDescending(element.zOrder())
          .setMinZoom(10);
      }
    }
  }

  @Override
  public void process(Tables.OsmHighwayLinestring element, FeatureCollector features) {
    String ref = element.ref();
    List<Transportation.RouteRelation> relations = transportation.getRouteRelations(element);
    Transportation.RouteRelation relation = relations.isEmpty() ? null : relations.get(0);
    if (relation != null && nullIfEmpty(relation.ref()) != null) {
      ref = relation.ref();
    }

    String name = nullIfEmpty(element.name());
    ref = nullIfEmpty(ref);
    String highway = nullIfEmpty(element.highway());

    String highwayClass = highwayClass(element.highway(), null, element.construction(), element.manMade());
    if (element.isArea() || highway == null || highwayClass == null || (name == null && ref == null)) {
      return;
    }

    boolean isLink = Transportation.isLink(highway);
    String baseClass = highwayClass.replace("_construction", "");

    int minzoom = FieldValues.CLASS_TRUNK.equals(baseClass) ? 8 :
      FieldValues.CLASS_MOTORWAY.equals(baseClass) ? 6 :
      isLink ? 13 : 12; // fallback - get from line minzoom, but floor at 12

    // inherit min zoom threshold from visible road, and ensure we never show a label on a road that's not visible yet.
    minzoom = Math.max(minzoom, transportation.getMinzoom(element, highwayClass));

    FeatureCollector.Feature feature = features.line(LAYER_NAME)
      .setBufferPixels(BUFFER_SIZE)
      .setBufferPixelOverrides(MIN_LENGTH)
      // TODO abbreviate road names - can't port osml10n because it is AGPL
      .putAttrs(LanguageUtils.getNamesWithoutTranslations(element.source().tags()))
      .setAttr(Fields.REF, ref)
      .setAttr(Fields.REF_LENGTH, ref != null ? ref.length() : null)
      .setAttr(Fields.NETWORK,
        (relation != null && relation.networkType() != null) ? relation.networkType().name :
          !nullOrEmpty(ref) ? "road" : null)
      .setAttr(Fields.CLASS, highwayClass)
      .setAttr(Fields.SUBCLASS, highwaySubclass(highwayClass, null, highway))
      .setMinPixelSize(0)
      .setSortKey(element.zOrder())
      .setMinZoom(minzoom);

    // populate route_1, route_2, ... tags
    for (int i = 0; i < Math.min(CONCURRENT_ROUTE_KEYS.size(), relations.size()); i++) {
      Transportation.RouteRelation routeRelation = relations.get(i);
      feature.setAttr(CONCURRENT_ROUTE_KEYS.get(i), routeRelation.network() == null ? null :
        routeRelation.network() + "=" + coalesce(routeRelation.ref(), ""));
    }

    if (brunnel) {
      feature.setAttr(Fields.BRUNNEL, brunnel(element.isBridge(), element.isTunnel(), element.isFord()));
    }

    /*
     * to help group roads into longer segments, add temporary tags to limit which segments get grouped together. Since
     * a divided highway typically has a separate relation for each direction, this ends up keeping segments going
     * opposite directions group getting grouped together and confusing the line merging process
     */
    if (limitMerge) {
      feature
        .setAttr(LINK_TEMP_KEY, isLink ? 1 : 0)
        .setAttr(RELATION_ID_TEMP_KEY, relation == null ? null : relation.id());
    }

    if (isFootwayOrSteps(highway)) {
      feature
        .setAttrWithMinzoom(Fields.LAYER, nullIfLong(element.layer(), 0), 12)
        .setAttrWithMinzoom(Fields.LEVEL, Parse.parseLongOrNull(element.source().getTag("level")), 12)
        .setAttrWithMinzoom(Fields.INDOOR, element.indoor() ? 1 : null, 12);
    }
  }

  @Override
  public void process(Tables.OsmAerialwayLinestring element, FeatureCollector features) {
    if (!nullOrEmpty(element.name())) {
      features.line(LAYER_NAME)
        .setBufferPixels(BUFFER_SIZE)
        .setBufferPixelOverrides(MIN_LENGTH)
        .putAttrs(LanguageUtils.getNamesWithoutTranslations(element.source().tags()))
        .setAttr(Fields.CLASS, "aerialway")
        .setAttr(Fields.SUBCLASS, element.aerialway())
        .setMinPixelSize(0)
        .setSortKey(element.zOrder())
        .setMinZoom(12);
    }
  }

  @Override
  public void process(Tables.OsmShipwayLinestring element, FeatureCollector features) {
    if (!nullOrEmpty(element.name())) {
      features.line(LAYER_NAME)
        .setBufferPixels(BUFFER_SIZE)
        .setBufferPixelOverrides(MIN_LENGTH)
        .putAttrs(LanguageUtils.getNamesWithoutTranslations(element.source().tags()))
        .setAttr(Fields.CLASS, element.shipway())
        .setMinPixelSize(0)
        .setSortKey(element.zOrder())
        .setMinZoom(12);
    }
  }

  @Override
  public List<VectorTile.Feature> postProcess(int zoom, List<VectorTile.Feature> items) {
    double tolerance = config.tolerance(zoom);
    double minLength = coalesce(MIN_LENGTH.apply(zoom), 0).doubleValue();
    // TODO tolerances:
    // z6: (tolerance: 500)
    // z7: (tolerance: 200)
    // z8: (tolerance: 120)
    // z9-11: (tolerance: 50)
    Function<Map<String, Object>, Double> lengthLimitCalculator =
      zoom >= 14 ? (p -> 0d) :
        minLength > 0 ? (p -> minLength) :
        this::getMinLengthForName;
    var result = FeatureMerge.mergeLineStrings(items, lengthLimitCalculator, tolerance, BUFFER_SIZE);
    if (limitMerge) {
      // remove temp keys that were just used to improve line merging
      for (var feature : result) {
        feature.attrs().remove(LINK_TEMP_KEY);
        feature.attrs().remove(RELATION_ID_TEMP_KEY);
      }
    }
    return result;
  }

  /** Returns the minimum pixel length that a name will fit into. */
  private double getMinLengthForName(Map<String, Object> attrs) {
    Object ref = attrs.get(Fields.REF);
    Object name = coalesce(attrs.get(Fields.NAME), ref);
    return (sizeForShield && ref instanceof String) ? 6 :
      name instanceof String str ? str.length() * 6 : Double.MAX_VALUE;
  }

  private enum HighwayClass {
    MOTORWAY("motorway", 6),
    TRUNK("trunk", 5),
    PRIMARY("primary", 4),
    SECONDARY("secondary", 3),
    TERTIARY("tertiary", 2),
    UNCLASSIFIED("unclassified", 1),
    UNKNOWN("", 0);

    private static final Map<String, HighwayClass> indexByString = new HashMap<>();
    private static final Map<Byte, HighwayClass> indexByByte = new HashMap<>();
    final byte value;
    final String highwayValue;

    HighwayClass(String highwayValue, int id) {
      this.highwayValue = highwayValue;
      this.value = (byte) id;
    }

    static {
      Arrays.stream(values()).forEach(cls -> {
        indexByString.put(cls.highwayValue, cls);
        indexByByte.put(cls.value, cls);
      });
    }

    static HighwayClass from(String highway) {
      return indexByString.getOrDefault(highway, UNKNOWN);
    }

    static HighwayClass from(byte value) {
      return indexByByte.getOrDefault(value, UNKNOWN);
    }
  }
}
