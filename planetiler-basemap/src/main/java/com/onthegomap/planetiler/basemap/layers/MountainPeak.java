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

import static com.onthegomap.planetiler.basemap.util.Utils.elevationTags;
import static com.onthegomap.planetiler.basemap.util.Utils.nullIfEmpty;

import com.carrotsearch.hppc.LongIntMap;
import com.onthegomap.planetiler.FeatureCollector;
import com.onthegomap.planetiler.VectorTile;
import com.onthegomap.planetiler.basemap.BasemapProfile;
import com.onthegomap.planetiler.basemap.generated.OpenMapTilesSchema;
import com.onthegomap.planetiler.basemap.generated.Tables;
import com.onthegomap.planetiler.basemap.util.LanguageUtils;
import com.onthegomap.planetiler.collection.Hppc;
import com.onthegomap.planetiler.config.PlanetilerConfig;
import com.onthegomap.planetiler.geo.GeometryException;
import com.onthegomap.planetiler.reader.SourceFeature;
import com.onthegomap.planetiler.stats.Stats;
import com.onthegomap.planetiler.util.Parse;
import com.onthegomap.planetiler.util.Translations;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.prep.PreparedGeometry;
import org.locationtech.jts.geom.prep.PreparedGeometryFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Defines the logic for generating map elements for mountain peak label points in the {@code mountain_peak} layer from
 * source features.
 * <p>
 * This class is ported to Java from
 * <a href="https://github.com/openmaptiles/openmaptiles/tree/master/layers/mountain_peak">OpenMapTiles mountain_peak
 * sql files</a>.
 */
public class MountainPeak implements
  BasemapProfile.NaturalEarthProcessor,
  OpenMapTilesSchema.MountainPeak,
  Tables.OsmPeakPoint.Handler,
  Tables.OsmMountainLinestring.Handler,
  BasemapProfile.FeaturePostProcessor {

  /*
   * Mountain peaks come from OpenStreetMap data and are ranked by importance (based on if they
   * have a name or wikipedia page) then by elevation.  Uses the "label grid" feature to limit
   * label density by only taking the top 5 most important mountain peaks within each 100x100px
   * square.
   */
  private static final Logger LOGGER = LoggerFactory.getLogger(MountainPeak.class);

  private final Translations translations;
  private final Stats stats;
  // keep track of areas that prefer feet to meters to set customary_ft=1 (just U.S.)
  private PreparedGeometry unitedStates = null;
  private final AtomicBoolean loggedNoUS = new AtomicBoolean(false);

  public MountainPeak(Translations translations, PlanetilerConfig config, Stats stats) {
    this.translations = translations;
    this.stats = stats;
  }

  @Override
  public void processNaturalEarth(String table, SourceFeature feature, FeatureCollector features) {
    if ("ne_10m_admin_0_countries".equals(table) && feature.hasTag("iso_a2", "US")) {
      // multiple threads call this method concurrently, US polygon *should* only be found
      // once, but just to be safe synchronize updates to that field
      synchronized (this) {
        try {
          Geometry boundary = feature.polygon();
          unitedStates = PreparedGeometryFactory.prepare(boundary);
        } catch (GeometryException e) {
          LOGGER.error("Failed to get United States Polygon for mountain_peak layer: " + e);
        }
      }
    }
  }

  @Override
  public void process(Tables.OsmPeakPoint element, FeatureCollector features) {
    Double meters = Parse.meters(element.ele());
    if (meters != null && Math.abs(meters) < 10_000) {
      var feature = features.point(LAYER_NAME)
        .setAttr(Fields.CLASS, element.source().getTag("natural"))
        .putAttrs(LanguageUtils.getNames(element.source().tags(), translations))
        .putAttrs(elevationTags(meters))
        .setSortKeyDescending(
          meters.intValue() +
            (nullIfEmpty(element.wikipedia()) != null ? 10_000 : 0) +
            (nullIfEmpty(element.name()) != null ? 10_000 : 0)
        )
        .setMinZoom(7)
        // need to use a larger buffer size to allow enough points through to not cut off
        // any label grid squares which could lead to inconsistent label ranks for a feature
        // in adjacent tiles. postProcess() will remove anything outside the desired buffer.
        .setBufferPixels(100)
        .setPointLabelGridSizeAndLimit(13, 100, 5);

      if (peakInAreaUsingFeet(element)) {
        feature.setAttr(Fields.CUSTOMARY_FT, 1);
      }
    }
  }

  @Override
  public void process(Tables.OsmMountainLinestring element, FeatureCollector features) {
    // TODO rank is approximate to sort important/named ridges before others, should switch to labelgrid for linestrings later
    int rank = 3 -
      (nullIfEmpty(element.wikipedia()) != null ? 1 : 0) -
      (nullIfEmpty(element.name()) != null ? 1 : 0);
    features.line(LAYER_NAME)
      .setAttr(Fields.CLASS, element.source().getTag("natural"))
      .setAttr(Fields.RANK, rank)
      .putAttrs(LanguageUtils.getNames(element.source().tags(), translations))
      .setSortKey(rank)
      .setMinZoom(13)
      .setBufferPixels(100);
  }

  /** Returns true if {@code element} is a point in an area where feet are used insead of meters (the US). */
  private boolean peakInAreaUsingFeet(Tables.OsmPeakPoint element) {
    if (unitedStates == null) {
      if (!loggedNoUS.get() && loggedNoUS.compareAndSet(false, true)) {
        LOGGER.warn("No US polygon for inferring mountain_peak customary_ft tag");
      }
    } else {
      try {
        Geometry wayGeometry = element.source().worldGeometry();
        return unitedStates.intersects(wayGeometry);
      } catch (GeometryException e) {
        e.log(stats, "omt_mountain_peak_us_test",
          "Unable to test mountain_peak against US polygon: " + element.source().id());
      }
    }
    return false;
  }

  @Override
  public List<VectorTile.Feature> postProcess(int zoom, List<VectorTile.Feature> items) {
    LongIntMap groupCounts = Hppc.newLongIntHashMap();
    for (int i = 0; i < items.size(); i++) {
      VectorTile.Feature feature = items.get(i);
      int gridrank = groupCounts.getOrDefault(feature.group(), 1);
      groupCounts.put(feature.group(), gridrank + 1);
      // now that we have accurate ranks, remove anything outside the desired buffer
      if (!insideTileBuffer(feature)) {
        items.set(i, null);
      } else if (!feature.attrs().containsKey(Fields.RANK)) {
        feature.attrs().put(Fields.RANK, gridrank);
      }
    }
    return items;
  }

  private static boolean insideTileBuffer(double xOrY) {
    return xOrY >= -BUFFER_SIZE && xOrY <= 256 + BUFFER_SIZE;
  }

  private boolean insideTileBuffer(VectorTile.Feature feature) {
    try {
      Geometry geom = feature.geometry().decode();
      return !(geom instanceof Point point) || (insideTileBuffer(point.getX()) && insideTileBuffer(point.getY()));
    } catch (GeometryException e) {
      e.log(stats, "mountain_peak_decode_point", "Error decoding mountain peak point: " + feature.attrs());
      return false;
    }
  }
}
