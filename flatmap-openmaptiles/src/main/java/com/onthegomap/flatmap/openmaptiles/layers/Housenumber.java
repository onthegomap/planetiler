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

import com.onthegomap.flatmap.FeatureCollector;
import com.onthegomap.flatmap.config.FlatmapConfig;
import com.onthegomap.flatmap.openmaptiles.generated.OpenMapTilesSchema;
import com.onthegomap.flatmap.openmaptiles.generated.Tables;
import com.onthegomap.flatmap.stats.Stats;
import com.onthegomap.flatmap.util.Translations;

/**
 * This class is ported to Java from https://github.com/openmaptiles/openmaptiles/tree/master/layers/housenumber
 */
public class Housenumber implements OpenMapTilesSchema.Housenumber, Tables.OsmHousenumberPoint.Handler {

  public Housenumber(Translations translations, FlatmapConfig config, Stats stats) {
  }

  @Override
  public void process(Tables.OsmHousenumberPoint element, FeatureCollector features) {
    features.centroidIfConvex(LAYER_NAME)
      .setBufferPixels(BUFFER_SIZE)
      .setAttr(Fields.HOUSENUMBER, element.housenumber())
      .setZoomRange(14, 14);
  }
}
