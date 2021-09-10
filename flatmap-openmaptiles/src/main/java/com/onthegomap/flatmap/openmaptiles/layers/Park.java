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
import static com.onthegomap.flatmap.collection.FeatureGroup.Z_ORDER_MIN;
import static com.onthegomap.flatmap.openmaptiles.Utils.coalesce;
import static com.onthegomap.flatmap.openmaptiles.Utils.nullIfEmpty;

import com.carrotsearch.hppc.LongIntHashMap;
import com.carrotsearch.hppc.LongIntMap;
import com.onthegomap.flatmap.FeatureCollector;
import com.onthegomap.flatmap.VectorTile;
import com.onthegomap.flatmap.config.FlatmapConfig;
import com.onthegomap.flatmap.geo.GeoUtils;
import com.onthegomap.flatmap.geo.GeometryException;
import com.onthegomap.flatmap.geo.GeometryType;
import com.onthegomap.flatmap.openmaptiles.LanguageUtils;
import com.onthegomap.flatmap.openmaptiles.OpenMapTilesProfile;
import com.onthegomap.flatmap.openmaptiles.generated.OpenMapTilesSchema;
import com.onthegomap.flatmap.openmaptiles.generated.Tables;
import com.onthegomap.flatmap.stats.Stats;
import com.onthegomap.flatmap.util.Translations;
import java.util.List;
import java.util.Locale;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is ported to Java from https://github.com/openmaptiles/openmaptiles/tree/master/layers/park
 */
public class Park implements
  OpenMapTilesSchema.Park,
  Tables.OsmParkPolygon.Handler,
  OpenMapTilesProfile.FeaturePostProcessor {

  private static final Logger LOGGER = LoggerFactory.getLogger(Park.class);
  private final Translations translations;
  private final Stats stats;

  public Park(Translations translations, FlatmapConfig config, Stats stats) {
    this.stats = stats;
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
          .putAttrs(LanguageUtils.getNames(element.source().tags(), translations))
          .setPointLabelGridPixelSize(14, 100)
          .setZorder(Z_ORDER_MIN +
            ("national_park".equals(clazz) ? PARK_NATIONAL_PARK_BOOST : 0) +
            ((element.source().hasTag("wikipedia") || element.source().hasTag("wikidata")) ? PARK_WIKIPEDIA_BOOST
              : 0) +
            areaBoost
          ).setZoomRange(minzoom, 14);
      } catch (GeometryException e) {
        e.log(stats, "omt_park_area", "Unable to get park area for " + element.source().id());
      }
    }
  }

  @Override
  public List<VectorTile.Feature> postProcess(int zoom, List<VectorTile.Feature> items) {
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
