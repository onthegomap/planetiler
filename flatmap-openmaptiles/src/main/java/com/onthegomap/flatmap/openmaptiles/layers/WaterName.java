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

import static com.onthegomap.flatmap.openmaptiles.Utils.nullIfEmpty;

import com.carrotsearch.hppc.LongObjectMap;
import com.graphhopper.coll.GHLongObjectHashMap;
import com.onthegomap.flatmap.FeatureCollector;
import com.onthegomap.flatmap.Translations;
import com.onthegomap.flatmap.config.Arguments;
import com.onthegomap.flatmap.geo.GeoUtils;
import com.onthegomap.flatmap.geo.GeometryException;
import com.onthegomap.flatmap.openmaptiles.LanguageUtils;
import com.onthegomap.flatmap.openmaptiles.OpenMapTilesProfile;
import com.onthegomap.flatmap.openmaptiles.generated.OpenMapTilesSchema;
import com.onthegomap.flatmap.openmaptiles.generated.Tables;
import com.onthegomap.flatmap.reader.SourceFeature;
import com.onthegomap.flatmap.stats.Stats;
import com.onthegomap.flatmap.util.Parse;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;
import org.locationtech.jts.geom.Geometry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is ported to Java from https://github.com/openmaptiles/openmaptiles/tree/master/layers/water_name
 */
public class WaterName implements OpenMapTilesSchema.WaterName,
  Tables.OsmMarinePoint.Handler,
  Tables.OsmWaterPolygon.Handler,
  OpenMapTilesProfile.NaturalEarthProcessor,
  OpenMapTilesProfile.LakeCenterlineProcessor {

  private static final Logger LOGGER = LoggerFactory.getLogger(WaterName.class);

  private final Translations translations;
  // need to synchronize updates from multiple threads
  private final LongObjectMap<Geometry> lakeCenterlines = new GHLongObjectHashMap<>();
  // may be updated concurrently by multiple threads
  private final ConcurrentSkipListMap<String, Integer> importantMarinePoints = new ConcurrentSkipListMap<>();
  private final Stats stats;

  @Override
  public void release() {
    lakeCenterlines.release();
    importantMarinePoints.clear();
  }

  public WaterName(Translations translations, Arguments args, Stats stats) {
    this.translations = translations;
    this.stats = stats;
  }

  @Override
  public void processNaturalEarth(String table, SourceFeature feature,
    FeatureCollector features) {
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
  public void processLakeCenterline(SourceFeature feature, FeatureCollector features) {
    long osmId = Math.abs(feature.getLong("OSM_ID"));
    if (osmId == 0L) {
      LOGGER.warn("Bad lake centerline. Tags: " + feature.tags());
    } else {
      try {
        // multiple threads call this concurrently
        synchronized (this) {
          lakeCenterlines.put(osmId, feature.worldGeometry());
        }
      } catch (GeometryException e) {
        e.log(stats, "omt_water_name_lakeline", "Bad lake centerline: " + feature);
      }
    }
  }

  @Override
  public void process(Tables.OsmMarinePoint element, FeatureCollector features) {
    if (!element.name().isBlank()) {
      String place = element.place();
      var source = element.source();
      // use name from OSM, but min zoom from natural earth if it exists
      Integer rank = Parse.parseIntOrNull(source.getTag("rank"));
      Integer nerank;
      String name = element.name().toLowerCase();
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
        .setAttrs(LanguageUtils.getNames(source.tags(), translations))
        .setAttr(Fields.CLASS, place)
        .setAttr(Fields.INTERMITTENT, element.isIntermittent() ? 1 : 0)
        .setZoomRange(minZoom, 14);
    }
  }

  private static final double WORLD_AREA_FOR_70K_SQUARE_METERS =
    Math.pow(GeoUtils.metersToPixelAtEquator(0, Math.sqrt(70_000)) / 256d, 2);
  private static final double LOG2 = Math.log(2);

  @Override
  public void process(Tables.OsmWaterPolygon element, FeatureCollector features) {
    if (nullIfEmpty(element.name()) != null) {
      try {
        Geometry centerlineGeometry = lakeCenterlines.get(element.source().id());
        FeatureCollector.Feature feature;
        int minzoom = 9;
        if (centerlineGeometry != null) {
          feature = features.geometry(LAYER_NAME, centerlineGeometry)
            .setMinPixelSizeBelowZoom(13, 6 * element.name().length());
        } else {
          feature = features.pointOnSurface(LAYER_NAME);
          Geometry geometry = element.source().worldGeometry();
          double area = geometry.getArea();
          minzoom = (int) Math.floor(20 - Math.log(area / WORLD_AREA_FOR_70K_SQUARE_METERS) / LOG2);
          minzoom = Math.min(14, Math.max(9, minzoom));
        }
        feature
          .setAttr(Fields.CLASS, FieldValues.CLASS_LAKE)
          .setBufferPixels(BUFFER_SIZE)
          .setAttrs(LanguageUtils.getNames(element.source().tags(), translations))
          .setAttr(Fields.INTERMITTENT, element.isIntermittent() ? 1 : 0)
          .setZoomRange(minzoom, 14);
      } catch (GeometryException e) {
        e.log(stats, "omt_water_polygon", "Unable to get geometry for water polygon " + element.source().id());
      }
    }
  }
}
