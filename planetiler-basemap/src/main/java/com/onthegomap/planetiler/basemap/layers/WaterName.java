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

import com.carrotsearch.hppc.LongObjectMap;
import com.onthegomap.planetiler.FeatureCollector;
import com.onthegomap.planetiler.basemap.BasemapProfile;
import com.onthegomap.planetiler.basemap.generated.OpenMapTilesSchema;
import com.onthegomap.planetiler.basemap.generated.Tables;
import com.onthegomap.planetiler.basemap.util.LanguageUtils;
import com.onthegomap.planetiler.collection.Hppc;
import com.onthegomap.planetiler.config.PlanetilerConfig;
import com.onthegomap.planetiler.geo.GeoUtils;
import com.onthegomap.planetiler.geo.GeometryException;
import com.onthegomap.planetiler.reader.SourceFeature;
import com.onthegomap.planetiler.stats.Stats;
import com.onthegomap.planetiler.util.Parse;
import com.onthegomap.planetiler.util.Translations;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;
import org.locationtech.jts.geom.Geometry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Defines the logic for generating map elements for ocean and lake names in the {@code water_name} layer from source
 * features.
 * <p>
 * This class is ported to Java from
 * <a href="https://github.com/openmaptiles/openmaptiles/tree/master/layers/water_name">OpenMapTiles water_name sql
 * files</a>.
 */
public class WaterName implements
  OpenMapTilesSchema.WaterName,
  Tables.OsmMarinePoint.Handler,
  Tables.OsmWaterPolygon.Handler,
  BasemapProfile.NaturalEarthProcessor,
  BasemapProfile.LakeCenterlineProcessor {

  /*
   * Labels for lakes and oceans come primarily from OpenStreetMap data, but we also join
   * with the lake centerlines source to get linestring geometries for prominent lakes.
   * We also join with natural earth to make certain important lake/ocean labels visible
   * at lower zoom levels.
   */

  private static final Logger LOGGER = LoggerFactory.getLogger(WaterName.class);
  private static final double WORLD_AREA_FOR_70K_SQUARE_METERS =
    Math.pow(GeoUtils.metersToPixelAtEquator(0, Math.sqrt(70_000)) / 256d, 2);
  private static final double LOG2 = Math.log(2);
  private final Translations translations;
  // need to synchronize updates from multiple threads
  private final LongObjectMap<Geometry> lakeCenterlines = Hppc.newLongObjectHashMap();
  // may be updated concurrently by multiple threads
  private final ConcurrentSkipListMap<String, Integer> importantMarinePoints = new ConcurrentSkipListMap<>();
  private final Stats stats;

  public WaterName(Translations translations, PlanetilerConfig config, Stats stats) {
    this.translations = translations;
    this.stats = stats;
  }

  @Override
  public void release() {
    lakeCenterlines.release();
    importantMarinePoints.clear();
  }

  @Override
  public void processLakeCenterline(SourceFeature feature, FeatureCollector features) {
    // TODO pull lake centerline computation into planetiler?
    long osmId = Math.abs(feature.getLong("OSM_ID"));
    if (osmId == 0L) {
      LOGGER.warn("Bad lake centerline. Tags: " + feature.tags());
    } else {
      try {
        // multiple threads call this concurrently
        synchronized (this) {
          // if we already have a centerline for this OSM_ID, then merge the existing one with this one
          var newGeometry = feature.worldGeometry();
          var oldGeometry = lakeCenterlines.get(osmId);
          if (oldGeometry != null) {
            newGeometry = GeoUtils.combine(oldGeometry, newGeometry);
          }
          lakeCenterlines.put(osmId, newGeometry);
        }
      } catch (GeometryException e) {
        e.log(stats, "omt_water_name_lakeline", "Bad lake centerline: " + feature);
      }
    }
  }

  @Override
  public void processNaturalEarth(String table, SourceFeature feature, FeatureCollector features) {
    // use natural earth named polygons just as a source of name to zoom-level mappings for later
    if ("ne_10m_geography_marine_polys".equals(table)) {
      String name = feature.getString("name");
      Integer scalerank = Parse.parseIntOrNull(feature.getTag("scalerank"));
      if (name != null && scalerank != null) {
        name = name.replaceAll("\\s+", " ").trim().toLowerCase();
        importantMarinePoints.put(name, scalerank);
      }
    }
  }

  @Override
  public void process(Tables.OsmMarinePoint element, FeatureCollector features) {
    if (!element.name().isBlank()) {
      String place = element.place();
      var source = element.source();
      // use name from OSM, but get min zoom from natural earth based on fuzzy name match...
      Integer rank = Parse.parseIntOrNull(source.getTag("rank"));
      String name = element.name().toLowerCase();
      Integer nerank;
      if ((nerank = importantMarinePoints.get(name)) != null) {
        rank = nerank;
      } else if ((nerank = importantMarinePoints.get(source.getString("name:en", "").toLowerCase())) != null) {
        rank = nerank;
      } else if ((nerank = importantMarinePoints.get(source.getString("name:es", "").toLowerCase())) != null) {
        rank = nerank;
      } else {
        Map.Entry<String, Integer> next = importantMarinePoints.ceilingEntry(name);
        if (next != null && next.getKey().startsWith(name)) {
          rank = next.getValue();
        }
      }
      int minZoom = "ocean".equals(place) ? 0 : rank != null ? rank : 8;
      features.point(LAYER_NAME)
        .setBufferPixels(BUFFER_SIZE)
        .putAttrs(LanguageUtils.getNames(source.tags(), translations))
        .setAttr(Fields.CLASS, place)
        .setAttr(Fields.INTERMITTENT, element.isIntermittent() ? 1 : 0)
        .setMinZoom(minZoom);
    }
  }

  @Override
  public void process(Tables.OsmWaterPolygon element, FeatureCollector features) {
    if (nullIfEmpty(element.name()) != null) {
      try {
        Geometry centerlineGeometry = lakeCenterlines.get(element.source().id());
        FeatureCollector.Feature feature;
        int minzoom = 9;
        if (centerlineGeometry != null) {
          // prefer lake centerline if it exists
          feature = features.geometry(LAYER_NAME, centerlineGeometry)
            .setMinPixelSizeBelowZoom(13, 6 * element.name().length());
        } else {
          // otherwise just use a label point inside the lake
          feature = features.pointOnSurface(LAYER_NAME);
          Geometry geometry = element.source().worldGeometry();
          double area = geometry.getArea();
          minzoom = (int) Math.floor(20 - Math.log(area / WORLD_AREA_FOR_70K_SQUARE_METERS) / LOG2);
          minzoom = Math.min(14, Math.max(9, minzoom));
        }
        feature
          .setAttr(Fields.CLASS, FieldValues.CLASS_LAKE)
          .setBufferPixels(BUFFER_SIZE)
          .putAttrs(LanguageUtils.getNames(element.source().tags(), translations))
          .setAttr(Fields.INTERMITTENT, element.isIntermittent() ? 1 : 0)
          .setMinZoom(minzoom);
      } catch (GeometryException e) {
        e.log(stats, "omt_water_polygon", "Unable to get geometry for water polygon " + element.source().id());
      }
    }
  }
}
