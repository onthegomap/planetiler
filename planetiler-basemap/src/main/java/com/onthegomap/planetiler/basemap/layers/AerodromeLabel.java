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
import static com.onthegomap.planetiler.basemap.util.Utils.nullOrEmpty;

import com.onthegomap.planetiler.FeatureCollector;
import com.onthegomap.planetiler.basemap.generated.OpenMapTilesSchema;
import com.onthegomap.planetiler.basemap.generated.Tables;
import com.onthegomap.planetiler.basemap.util.LanguageUtils;
import com.onthegomap.planetiler.basemap.util.Utils;
import com.onthegomap.planetiler.config.PlanetilerConfig;
import com.onthegomap.planetiler.expression.MultiExpression;
import com.onthegomap.planetiler.stats.Stats;
import com.onthegomap.planetiler.util.Translations;

/**
 * Defines the logic for generating map elements in the {@code aerodrome_label} layer from source
 * features.
 *
 * <p>This class is ported to Java from <a
 * href="https://github.com/openmaptiles/openmaptiles/tree/master/layers/aerodrome_label">OpenMapTiles
 * aerodrome_layer sql files</a>.
 */
public class AerodromeLabel
    implements OpenMapTilesSchema.AerodromeLabel, Tables.OsmAerodromeLabelPoint.Handler {

  private final MultiExpression.Index<String> classLookup;
  private final Translations translations;

  public AerodromeLabel(Translations translations, PlanetilerConfig config, Stats stats) {
    this.classLookup = FieldMappings.Class.index();
    this.translations = translations;
  }

  @Override
  public void process(Tables.OsmAerodromeLabelPoint element, FeatureCollector features) {
    String clazz = classLookup.getOrElse(element.source(), FieldValues.CLASS_OTHER);
    boolean important =
        !nullOrEmpty(element.iata()) && FieldValues.CLASS_INTERNATIONAL.equals(clazz);
    features
        .centroid(LAYER_NAME)
        .setBufferPixels(BUFFER_SIZE)
        .setMinZoom(important ? 8 : 10)
        .putAttrs(LanguageUtils.getNames(element.source().tags(), translations))
        .putAttrs(Utils.elevationTags(element.ele()))
        .setAttr(Fields.IATA, nullIfEmpty(element.iata()))
        .setAttr(Fields.ICAO, nullIfEmpty(element.icao()))
        .setAttr(Fields.CLASS, clazz);
  }
}
