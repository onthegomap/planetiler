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

import com.onthegomap.planetiler.FeatureCollector;
import com.onthegomap.planetiler.basemap.BasemapProfile;
import com.onthegomap.planetiler.basemap.generated.OpenMapTilesSchema;
import com.onthegomap.planetiler.basemap.generated.Tables;
import com.onthegomap.planetiler.basemap.util.Utils;
import com.onthegomap.planetiler.config.PlanetilerConfig;
import com.onthegomap.planetiler.expression.MultiExpression;
import com.onthegomap.planetiler.reader.SourceFeature;
import com.onthegomap.planetiler.stats.Stats;
import com.onthegomap.planetiler.util.Translations;

/**
 * Defines the logic for generating map elements for oceans and lakes in the {@code water} layer from source features.
 * <p>
 * This class is ported to Java from <a href="https://github.com/openmaptiles/openmaptiles/tree/master/layers/water">OpenMapTiles
 * water sql files</a>.
 */
public class Water implements
  OpenMapTilesSchema.Water,
  Tables.OsmWaterPolygon.Handler,
  BasemapProfile.NaturalEarthProcessor,
  BasemapProfile.OsmWaterPolygonProcessor {

  /*
   * At low zoom levels, use natural earth for oceans and major lakes, and at high zoom levels
   * use OpenStreetMap data. OpenStreetMap data contains smaller bodies of water, but not
   * large ocean polygons. For oceans, use https://osmdata.openstreetmap.de/data/water-polygons.html
   * which infers ocean polygons by preprocessing all coastline elements.
   */

  private final MultiExpression.Index<String> classMapping;

  public Water(Translations translations, PlanetilerConfig config, Stats stats) {
    this.classMapping = FieldMappings.Class.index();
  }

  @Override
  public void processNaturalEarth(String table, SourceFeature feature, FeatureCollector features) {
    record WaterInfo(int minZoom, int maxZoom, String clazz) {}
    WaterInfo info = switch (table) {
      case "ne_110m_ocean" -> new WaterInfo(0, 1, FieldValues.CLASS_OCEAN);
      case "ne_50m_ocean" -> new WaterInfo(2, 4, FieldValues.CLASS_OCEAN);
      case "ne_10m_ocean" -> new WaterInfo(5, 5, FieldValues.CLASS_OCEAN);

      case "ne_110m_lakes" -> new WaterInfo(0, 1, FieldValues.CLASS_LAKE);
      case "ne_50m_lakes" -> new WaterInfo(2, 3, FieldValues.CLASS_LAKE);
      case "ne_10m_lakes" -> new WaterInfo(4, 5, FieldValues.CLASS_LAKE);
      default -> null;
    };
    if (info != null) {
      features.polygon(LAYER_NAME)
        .setBufferPixels(BUFFER_SIZE)
        .setZoomRange(info.minZoom, info.maxZoom)
        .setAttr(Fields.CLASS, info.clazz);
    }
  }

  @Override
  public void processOsmWater(SourceFeature feature, FeatureCollector features) {
    features.polygon(LAYER_NAME)
      .setBufferPixels(BUFFER_SIZE)
      .setAttr(Fields.CLASS, FieldValues.CLASS_OCEAN)
      .setMinZoom(6);
  }

  @Override
  public void process(Tables.OsmWaterPolygon element, FeatureCollector features) {
    if (!"bay".equals(element.natural())) {
      features.polygon(LAYER_NAME)
        .setBufferPixels(BUFFER_SIZE)
        .setMinPixelSizeBelowZoom(11, 2)
        .setMinZoom(6)
        .setAttr(Fields.INTERMITTENT, element.isIntermittent() ? 1 : 0)
        .setAttrWithMinzoom(Fields.BRUNNEL, Utils.brunnel(element.isBridge(), element.isTunnel()), 12)
        .setAttr(Fields.CLASS, classMapping.getOrElse(element.source(), FieldValues.CLASS_RIVER));
    }
  }
}
