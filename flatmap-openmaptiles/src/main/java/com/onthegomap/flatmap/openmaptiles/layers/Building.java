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

import static com.onthegomap.flatmap.openmaptiles.Utils.coalesce;
import static com.onthegomap.flatmap.util.MemoryEstimator.CLASS_HEADER_BYTES;
import static com.onthegomap.flatmap.util.Parse.parseDoubleOrNull;
import static java.util.Map.entry;

import com.onthegomap.flatmap.FeatureCollector;
import com.onthegomap.flatmap.FeatureMerge;
import com.onthegomap.flatmap.VectorTile;
import com.onthegomap.flatmap.config.FlatmapConfig;
import com.onthegomap.flatmap.geo.GeometryException;
import com.onthegomap.flatmap.openmaptiles.OpenMapTilesProfile;
import com.onthegomap.flatmap.openmaptiles.generated.OpenMapTilesSchema;
import com.onthegomap.flatmap.openmaptiles.generated.Tables;
import com.onthegomap.flatmap.reader.osm.OsmElement;
import com.onthegomap.flatmap.reader.osm.OsmRelationInfo;
import com.onthegomap.flatmap.stats.Stats;
import com.onthegomap.flatmap.util.MemoryEstimator;
import com.onthegomap.flatmap.util.Translations;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * This class is ported to Java from https://github.com/openmaptiles/openmaptiles/tree/master/layers/building
 */
public class Building implements OpenMapTilesSchema.Building,
  Tables.OsmBuildingPolygon.Handler,
  OpenMapTilesProfile.FeaturePostProcessor,
  OpenMapTilesProfile.OsmRelationPreprocessor {

  private final boolean mergeZ13Buildings;

  private static final Map<String, String> MATERIAL_COLORS = Map.ofEntries(
    entry("cement_block", "#6a7880"),
    entry("brick", "#bd8161"),
    entry("plaster", "#dadbdb"),
    entry("wood", "#d48741"),
    entry("concrete", "#d3c2b0"),
    entry("metal", "#b7b1a6"),
    entry("stone", "#b4a995"),
    entry("mud", "#9d8b75"),
    entry("steel", "#b7b1a6"), // same as metal
    entry("glass", "#5a81a0"),
    entry("traditional", "#bd8161"), // same as brick
    entry("masonry", "#bd8161"), // same as brick
    entry("Brick", "#bd8161"), // same as brick
    entry("tin", "#b7b1a6"), // same as metal
    entry("timber_framing", "#b3b0a9"),
    entry("sandstone", "#b4a995"), // same as stone
    entry("clay", "#9d8b75") // same as mud
  );

  public Building(Translations translations, FlatmapConfig config, Stats stats) {
    this.mergeZ13Buildings = config.arguments().getBoolean(
      "building_merge_z13",
      "building layer: merge nearby buildings at z13",
      true
    );
  }

  private static record BuildingRelationInfo(long id) implements OsmRelationInfo {

    @Override
    public long estimateMemoryUsageBytes() {
      return CLASS_HEADER_BYTES + MemoryEstimator.estimateSizeLong(id);
    }
  }

  @Override
  public List<OsmRelationInfo> preprocessOsmRelation(OsmElement.Relation relation) {
    if (relation.hasTag("type", "building")) {
      return List.of(new BuildingRelationInfo(relation.id()));
    }
    return null;
  }

  @Override
  public void process(Tables.OsmBuildingPolygon element, FeatureCollector features) {
    Boolean hide3d = null;
    var relations = element.source().relationInfo(BuildingRelationInfo.class);
    for (var relation : relations) {
      if ("outline".equals(relation.role())) {
        hide3d = true;
        break;
      }
    }

    String color = element.colour();
    if (color == null && element.material() != null) {
      color = MATERIAL_COLORS.get(element.material());
    }
    if (color != null) {
      color = color.toLowerCase(Locale.ROOT);
    }

    Double height = coalesce(
      parseDoubleOrNull(element.height()),
      parseDoubleOrNull(element.buildingheight())
    );
    Double minHeight = coalesce(
      parseDoubleOrNull(element.minHeight()),
      parseDoubleOrNull(element.buildingminHeight())
    );
    Double levels = coalesce(
      parseDoubleOrNull(element.levels()),
      parseDoubleOrNull(element.buildinglevels())
    );
    Double minLevels = coalesce(
      parseDoubleOrNull(element.minLevel()),
      parseDoubleOrNull(element.buildingminLevel())
    );

    int renderHeight = (int) Math.ceil(height != null ? height
      : levels != null ? (levels * 3.66) : 5);
    int renderMinHeight = (int) Math.floor(minHeight != null ? minHeight
      : minLevels != null ? (minLevels * 3.66) : 0);

    if (renderHeight < 3660 && renderMinHeight < 3660) {
      var feature = features.polygon(LAYER_NAME).setBufferPixels(BUFFER_SIZE)
        .setZoomRange(13, 14)
        .setMinPixelSize(2)
        .setAttrWithMinzoom(Fields.RENDER_HEIGHT, renderHeight, 14)
        .setAttrWithMinzoom(Fields.RENDER_MIN_HEIGHT, renderMinHeight, 14)
        .setAttrWithMinzoom(Fields.COLOUR, color, 14)
        .setAttrWithMinzoom(Fields.HIDE_3D, hide3d, 14);
      if (mergeZ13Buildings) {
        feature
          .setMinPixelSize(0.1)
          .setPixelTolerance(0.25);
      }
    }
  }

  @Override
  public List<VectorTile.Feature> postProcess(int zoom,
    List<VectorTile.Feature> items) throws GeometryException {
    return (mergeZ13Buildings && zoom == 13) ? FeatureMerge.mergeNearbyPolygons(items, 4, 4, 0.5, 0.5) : items;
  }
}
