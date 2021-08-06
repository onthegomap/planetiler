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
import static com.onthegomap.flatmap.openmaptiles.Utils.nullIf;
import static com.onthegomap.flatmap.openmaptiles.Utils.nullIfEmpty;
import static com.onthegomap.flatmap.openmaptiles.Utils.nullOrEmpty;
import static java.util.Map.entry;

import com.carrotsearch.hppc.LongIntHashMap;
import com.carrotsearch.hppc.LongIntMap;
import com.onthegomap.flatmap.FeatureCollector;
import com.onthegomap.flatmap.Translations;
import com.onthegomap.flatmap.VectorTileEncoder;
import com.onthegomap.flatmap.config.Arguments;
import com.onthegomap.flatmap.geo.GeometryException;
import com.onthegomap.flatmap.openmaptiles.LanguageUtils;
import com.onthegomap.flatmap.openmaptiles.MultiExpression;
import com.onthegomap.flatmap.openmaptiles.OpenMapTilesProfile;
import com.onthegomap.flatmap.openmaptiles.generated.OpenMapTilesSchema;
import com.onthegomap.flatmap.openmaptiles.generated.Tables;
import com.onthegomap.flatmap.stats.Stats;
import com.onthegomap.flatmap.util.Parse;
import java.util.List;
import java.util.Map;

/**
 * This class is ported to Java from https://github.com/openmaptiles/openmaptiles/tree/master/layers/poi
 */
public class Poi implements OpenMapTilesSchema.Poi,
  Tables.OsmPoiPoint.Handler,
  Tables.OsmPoiPolygon.Handler,
  OpenMapTilesProfile.FeaturePostProcessor {

  private final MultiExpression.MultiExpressionIndex<String> classMapping;
  private final Translations translations;

  public Poi(Translations translations, Arguments args, Stats stats) {
    this.classMapping = FieldMappings.Class.index();
    this.translations = translations;
  }

  private String poiClass(String subclass, String mappingKey) {
    subclass = coalesce(subclass, "");
    return classMapping.getOrElse(Map.of(
      "subclass", subclass,
      "mapping_key", coalesce(mappingKey, "")
    ), subclass);
  }

  private static final Map<String, Integer> CLASS_RANKS = Map.ofEntries(
    entry(FieldValues.CLASS_HOSPITAL, 20),
    entry(FieldValues.CLASS_RAILWAY, 40),
    entry(FieldValues.CLASS_BUS, 50),
    entry(FieldValues.CLASS_ATTRACTION, 70),
    entry(FieldValues.CLASS_HARBOR, 75),
    entry(FieldValues.CLASS_COLLEGE, 80),
    entry(FieldValues.CLASS_SCHOOL, 85),
    entry(FieldValues.CLASS_STADIUM, 90),
    entry("zoo", 95),
    entry(FieldValues.CLASS_TOWN_HALL, 100),
    entry(FieldValues.CLASS_CAMPSITE, 110),
    entry(FieldValues.CLASS_CEMETERY, 115),
    entry(FieldValues.CLASS_PARK, 120),
    entry(FieldValues.CLASS_LIBRARY, 130),
    entry("police", 135),
    entry(FieldValues.CLASS_POST, 140),
    entry(FieldValues.CLASS_GOLF, 150),
    entry(FieldValues.CLASS_SHOP, 400),
    entry(FieldValues.CLASS_GROCERY, 500),
    entry(FieldValues.CLASS_FAST_FOOD, 600),
    entry(FieldValues.CLASS_CLOTHING_STORE, 700),
    entry(FieldValues.CLASS_BAR, 800)
  );

  static int poiClassRank(String clazz) {
    return CLASS_RANKS.getOrDefault(clazz, 1_000);
  }

  private int minzoom(String subclass, String mappingKey) {
    boolean lowZoom = ("station".equals(subclass) && "railway".equals(mappingKey)) ||
      "halt".equals(subclass) || "ferry_terminal".equals(subclass);
    return lowZoom ? 12 : 14;
  }

  @Override
  public void process(Tables.OsmPoiPoint element, FeatureCollector features) {
    // TODO handle uic_ref => agg_stop
    setupPoiFeature(element, features.point(LAYER_NAME));
  }

  @Override
  public void process(Tables.OsmPoiPolygon element, FeatureCollector features) {
    setupPoiFeature(element, features.centroidIfConvex(LAYER_NAME));
  }

  private <T extends Tables.WithSubclass & Tables.WithStation & Tables.WithFunicular & Tables.WithSport & Tables.WithInformation & Tables.WithReligion & Tables.WithMappingKey & Tables.WithName & Tables.WithIndoor & Tables.WithLayer & Tables.WithSource>
  void setupPoiFeature(T element, FeatureCollector.Feature output) {
    String rawSubclass = element.subclass();
    if ("station".equals(rawSubclass) && "subway".equals(element.station())) {
      rawSubclass = "subway";
    }
    if ("station".equals(rawSubclass) && "yes".equals(element.funicular())) {
      rawSubclass = "halt";
    }

    String subclass = switch (rawSubclass) {
      case "information" -> nullIfEmpty(element.information());
      case "place_of_worship" -> nullIfEmpty(element.religion());
      case "pitch" -> nullIfEmpty(element.sport());
      default -> rawSubclass;
    };
    String poiClass = poiClass(rawSubclass, element.mappingKey());
    int poiClassRank = poiClassRank(poiClass);
    int rankOrder = poiClassRank + ((nullOrEmpty(element.name())) ? 2000 : 0);

    output.setBufferPixels(BUFFER_SIZE)
      .setAttr(Fields.CLASS, poiClass)
      .setAttr(Fields.SUBCLASS, subclass)
      .setAttr(Fields.LAYER, nullIf(element.layer(), 0))
      .setAttr(Fields.LEVEL, Parse.parseLongOrNull(element.source().getTag("level")))
      .setAttr(Fields.INDOOR, element.indoor() ? 1 : null)
      .setAttrs(LanguageUtils.getNames(element.source().properties(), translations))
      .setLabelGridPixelSize(14, 64)
      .setZorder(-rankOrder)
      .setZoomRange(minzoom(element.subclass(), element.mappingKey()), 14);
  }

  @Override
  public List<VectorTileEncoder.Feature> postProcess(int zoom,
    List<VectorTileEncoder.Feature> items) throws GeometryException {
    LongIntMap groupCounts = new LongIntHashMap();
    for (int i = items.size() - 1; i >= 0; i--) {
      VectorTileEncoder.Feature feature = items.get(i);
      int gridrank = groupCounts.getOrDefault(feature.group(), 1);
      groupCounts.put(feature.group(), gridrank + 1);
      if (!feature.attrs().containsKey(Fields.RANK)) {
        feature.attrs().put(Fields.RANK, gridrank);
      }
    }
    return items;
  }
}
