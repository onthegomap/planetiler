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

import static com.onthegomap.planetiler.basemap.util.Utils.nullIfEmpty;

import com.carrotsearch.hppc.LongObjectHashMap;
import com.google.common.util.concurrent.AtomicDouble;
import com.onthegomap.planetiler.FeatureCollector;
import com.onthegomap.planetiler.FeatureMerge;
import com.onthegomap.planetiler.VectorTile;
import com.onthegomap.planetiler.basemap.BasemapProfile;
import com.onthegomap.planetiler.basemap.generated.OpenMapTilesSchema;
import com.onthegomap.planetiler.basemap.generated.Tables;
import com.onthegomap.planetiler.basemap.util.LanguageUtils;
import com.onthegomap.planetiler.basemap.util.Utils;
import com.onthegomap.planetiler.collection.Hppc;
import com.onthegomap.planetiler.config.PlanetilerConfig;
import com.onthegomap.planetiler.geo.GeometryException;
import com.onthegomap.planetiler.reader.SourceFeature;
import com.onthegomap.planetiler.reader.osm.OsmElement;
import com.onthegomap.planetiler.reader.osm.OsmReader;
import com.onthegomap.planetiler.reader.osm.OsmRelationInfo;
import com.onthegomap.planetiler.stats.Stats;
import com.onthegomap.planetiler.util.Translations;
import com.onthegomap.planetiler.util.ZoomFunction;
import java.util.List;
import java.util.Map;

/**
 * Defines the logic for generating river map elements in the {@code waterway} layer from source features.
 * <p>
 * This class is ported to Java from
 * <a href="https://github.com/openmaptiles/openmaptiles/tree/master/layers/waterway">OpenMapTiles waterway sql
 * files</a>.
 */
public class Waterway implements
  OpenMapTilesSchema.Waterway,
  Tables.OsmWaterwayLinestring.Handler,
  BasemapProfile.FeaturePostProcessor,
  BasemapProfile.NaturalEarthProcessor,
  BasemapProfile.OsmRelationPreprocessor,
  BasemapProfile.OsmAllProcessor {

  /*
   * Uses Natural Earth at lower zoom-levels and OpenStreetMap at higher zoom levels.
   *
   * For OpenStreetMap, attempts to merge disconnected linestrings with the same name
   * at lower zoom levels so that clients can more easily render the name. We also
   * limit their length at merge-time which only has visibilty into that feature in a
   * single tile, so at render-time we need to allow through features far enough outside
   * the tile boundary enough to not accidentally filter out a long river only because a
   * short segment of it goes through this tile.
   */

  private static final Map<String, Integer> CLASS_MINZOOM = Map.of(
    "river", 12,
    "canal", 12,

    "stream", 13,
    "drain", 13,
    "ditch", 13
  );
  private static final String TEMP_REL_ID_ADDR = "_relid";

  private final Translations translations;
  private final PlanetilerConfig config;
  private final Stats stats;
  private final LongObjectHashMap<AtomicDouble> riverRelationLengths = Hppc.newLongObjectHashMap();

  public Waterway(Translations translations, PlanetilerConfig config, Stats stats) {
    this.config = config;
    this.translations = translations;
    this.stats = stats;
  }

  private static final ZoomFunction.MeterToPixelThresholds MIN_PIXEL_LENGTHS = ZoomFunction.meterThresholds()
    .put(6, 500_000)
    .put(7, 400_000)
    .put(8, 300_000)
    .put(9, 8_000)
    .put(10, 4_000)
    .put(11, 1_000);

  // zoom-level 3-5 come from natural earth

  @Override
  public void processNaturalEarth(String table, SourceFeature feature, FeatureCollector features) {
    if (feature.hasTag("featurecla", "River")) {
      record ZoomRange(int min, int max) {}
      ZoomRange zoom = switch (table) {
        case "ne_110m_rivers_lake_centerlines" -> new ZoomRange(3, 3);
        case "ne_50m_rivers_lake_centerlines" -> new ZoomRange(4, 5);
        default -> null;
      };
      if (zoom != null) {
        features.line(LAYER_NAME)
          .setBufferPixels(BUFFER_SIZE)
          .setAttr(Fields.CLASS, FieldValues.CLASS_RIVER)
          .setZoomRange(zoom.min, zoom.max);
      }
    }
  }

  // zoom-level 6-8 come from OSM river relations

  private record WaterwayRelation(
    long id,
    Map<String, Object> names
  ) implements OsmRelationInfo {}

  @Override
  public List<OsmRelationInfo> preprocessOsmRelation(OsmElement.Relation relation) {
    if (relation.hasTag("waterway", "river") && !Utils.nullOrEmpty(relation.getString("name"))) {
      riverRelationLengths.put(relation.id(), new AtomicDouble());
      return List.of(new WaterwayRelation(relation.id(), LanguageUtils.getNames(relation.tags(), translations)));
    }
    return null;
  }

  @Override
  public void processAllOsm(SourceFeature feature, FeatureCollector features) {
    List<OsmReader.RelationMember<WaterwayRelation>> waterways = feature.relationInfo(WaterwayRelation.class);
    if (waterways != null && !waterways.isEmpty() && feature.canBeLine()) {
      for (var waterway : waterways) {
        String role = waterway.role();
        if (Utils.nullOrEmpty(role) || "main_stream".equals(role)) {
          long relId = waterway.relation().id();
          try {
            AtomicDouble counter = riverRelationLengths.get(relId);
            if (counter != null) {
              counter.addAndGet(feature.length());
            }
          } catch (GeometryException e) {
            e.log(stats, "waterway_decode", "Unable to get waterway length for " + feature.id());
          }
          features.line(LAYER_NAME)
            .setAttr(TEMP_REL_ID_ADDR, relId)
            .setBufferPixels(BUFFER_SIZE)
            .setAttr(Fields.CLASS, FieldValues.CLASS_RIVER)
            .putAttrs(waterway.relation().names())
            .setZoomRange(6, 8)
            .setMinPixelSize(0);
        }
      }
    }
  }

  // zoom-level 9+ come from OSM river ways

  @Override
  public void process(Tables.OsmWaterwayLinestring element, FeatureCollector features) {
    String waterway = element.waterway();
    String name = nullIfEmpty(element.name());
    boolean important = "river".equals(waterway) && name != null;
    int minzoom = important ? 9 : CLASS_MINZOOM.getOrDefault(element.waterway(), 14);
    features.line(LAYER_NAME)
      .setBufferPixels(BUFFER_SIZE)
      .setAttr(Fields.CLASS, element.waterway())
      .putAttrs(LanguageUtils.getNames(element.source().tags(), translations))
      .setMinZoom(minzoom)
      // details only at higher zoom levels so that named rivers can be merged more aggressively
      .setAttrWithMinzoom(Fields.BRUNNEL, Utils.brunnel(element.isBridge(), element.isTunnel()), 12)
      .setAttrWithMinzoom(Fields.INTERMITTENT, element.isIntermittent() ? 1 : 0, 12)
      // at lower zoom levels, we'll merge linestrings and limit length/clip afterwards
      .setBufferPixelOverrides(MIN_PIXEL_LENGTHS).setMinPixelSizeBelowZoom(11, 0);
  }

  @Override
  public List<VectorTile.Feature> postProcess(int zoom, List<VectorTile.Feature> items) {
    if (zoom >= 6 && zoom <= 8) {
      // remove ways for river relations if relation is not long enough
      double minSizeAtZoom = MIN_PIXEL_LENGTHS.apply(zoom).doubleValue() / Math.pow(2, zoom) / 256d;
      for (int i = 0; i < items.size(); i++) {
        Object relIdObj = items.get(i).attrs().remove(TEMP_REL_ID_ADDR);
        if (relIdObj instanceof Long relId && riverRelationLengths.get(relId).get() < minSizeAtZoom) {
          items.set(i, null);
        }
      }
      return FeatureMerge.mergeLineStrings(
        items,
        config.minFeatureSize(zoom),
        config.tolerance(zoom),
        BUFFER_SIZE
      );
    } else if (zoom >= 9 && zoom <= 11) {
      return FeatureMerge.mergeLineStrings(
        items,
        MIN_PIXEL_LENGTHS.apply(zoom).doubleValue(),
        config.tolerance(zoom),
        BUFFER_SIZE
      );
    }
    return items;
  }
}
