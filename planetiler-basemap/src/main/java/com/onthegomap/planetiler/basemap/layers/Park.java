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

import static com.onthegomap.planetiler.basemap.util.Utils.coalesce;
import static com.onthegomap.planetiler.basemap.util.Utils.nullIfEmpty;
import static com.onthegomap.planetiler.collection.FeatureGroup.SORT_KEY_BITS;

import com.carrotsearch.hppc.LongIntMap;
import com.onthegomap.planetiler.FeatureCollector;
import com.onthegomap.planetiler.FeatureMerge;
import com.onthegomap.planetiler.VectorTile;
import com.onthegomap.planetiler.basemap.BasemapProfile;
import com.onthegomap.planetiler.basemap.generated.OpenMapTilesSchema;
import com.onthegomap.planetiler.basemap.generated.Tables;
import com.onthegomap.planetiler.basemap.util.LanguageUtils;
import com.onthegomap.planetiler.collection.Hppc;
import com.onthegomap.planetiler.config.PlanetilerConfig;
import com.onthegomap.planetiler.geo.GeoUtils;
import com.onthegomap.planetiler.geo.GeometryException;
import com.onthegomap.planetiler.geo.GeometryType;
import com.onthegomap.planetiler.stats.Stats;
import com.onthegomap.planetiler.util.SortKey;
import com.onthegomap.planetiler.util.Translations;
import java.util.List;
import java.util.Locale;

/**
 * Defines the logic for generating map elements for designated parks polygons and their label points in the {@code
 * park} layer from source features.
 * <p>
 * This class is ported to Java from
 * <a href="https://github.com/openmaptiles/openmaptiles/tree/master/layers/park">OpenMapTiles park sql files</a>.
 */
public class Park implements
  OpenMapTilesSchema.Park,
  Tables.OsmParkPolygon.Handler,
  BasemapProfile.FeaturePostProcessor {

  // constants for packing the minimum zoom ordering of park labels into the sort-key field
  private static final int PARK_NATIONAL_PARK_BOOST = 1 << (SORT_KEY_BITS - 1);
  private static final int PARK_WIKIPEDIA_BOOST = 1 << (SORT_KEY_BITS - 2);

  // constants for determining the minimum zoom level for a park label based on its area
  private static final double WORLD_AREA_FOR_70K_SQUARE_METERS =
    Math.pow(GeoUtils.metersToPixelAtEquator(0, Math.sqrt(70_000)) / 256d, 2);
  private static final double LOG2 = Math.log(2);
  private static final int PARK_AREA_RANGE = 1 << (SORT_KEY_BITS - 3);
  private static final double SMALLEST_PARK_WORLD_AREA = Math.pow(4, -26); // 2^14 tiles, 2^12 pixels per tile

  private final Translations translations;
  private final Stats stats;

  public Park(Translations translations, PlanetilerConfig config, Stats stats) {
    this.stats = stats;
    this.translations = translations;
  }

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

    // park shape
    var outline = features.polygon(LAYER_NAME).setBufferPixels(BUFFER_SIZE)
      .setAttrWithMinzoom(Fields.CLASS, clazz, 5)
      .setMinPixelSize(2)
      .setMinZoom(4);

    // park name label point (if it has one)
    if (element.name() != null) {
      try {
        double area = element.source().area();
        int minzoom = getMinZoomForArea(area);

        var names = LanguageUtils.getNamesWithoutTranslations(element.source().tags());

        outline.putAttrsWithMinzoom(names, 5);

        features.pointOnSurface(LAYER_NAME).setBufferPixels(256)
          .setAttr(Fields.CLASS, clazz)
          .putAttrs(names)
          .putAttrs(LanguageUtils.getNames(element.source().tags(), translations))
          .setPointLabelGridPixelSize(14, 100)
          .setSortKey(SortKey
            .orderByTruesFirst("national_park".equals(clazz))
            .thenByTruesFirst(element.source().hasTag("wikipedia") || element.source().hasTag("wikidata"))
            .thenByLog(area, 1d, SMALLEST_PARK_WORLD_AREA, 1 << (SORT_KEY_BITS - 2) - 1)
            .get()
          ).setMinZoom(minzoom);
      } catch (GeometryException e) {
        e.log(stats, "omt_park_area", "Unable to get park area for " + element.source().id());
      }
    }
  }

  private int getMinZoomForArea(double area) {
    // sql filter:    area > 70000*2^(20-zoom_level)
    // simplifies to: zoom_level > 20 - log(area / 70000) / log(2)
    int minzoom = (int) Math.floor(20 - Math.log(area / WORLD_AREA_FOR_70K_SQUARE_METERS) / LOG2);
    minzoom = Math.min(14, Math.max(5, minzoom));
    return minzoom;
  }

  @Override
  public List<VectorTile.Feature> postProcess(int zoom, List<VectorTile.Feature> items) throws GeometryException {
    // infer the "rank" attribute from point ordering within each label grid square
    LongIntMap counts = Hppc.newLongIntHashMap();
    for (VectorTile.Feature feature : items) {
      if (feature.geometry().geomType() == GeometryType.POINT && feature.hasGroup()) {
        int count = counts.getOrDefault(feature.group(), 0) + 1;
        feature.attrs().put("rank", count);
        counts.put(feature.group(), count);
      }
    }
    if (zoom <= 4) {
      items = FeatureMerge.mergeOverlappingPolygons(items, 0);
    }
    return items;
  }
}
