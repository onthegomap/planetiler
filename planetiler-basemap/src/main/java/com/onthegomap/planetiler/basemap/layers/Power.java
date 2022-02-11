package com.onthegomap.planetiler.basemap.layers;

import com.onthegomap.planetiler.FeatureCollector;
import com.onthegomap.planetiler.basemap.BasemapProfile;
import com.onthegomap.planetiler.basemap.Layer;
import com.onthegomap.planetiler.config.PlanetilerConfig;
import com.onthegomap.planetiler.reader.SourceFeature;
import com.onthegomap.planetiler.stats.Stats;
import com.onthegomap.planetiler.util.Translations;

public class Power implements BasemapProfile.OsmAllProcessor, Layer {

  public static final String LAYER_NAME = "power";
  public static final int BUFFER_SIZE = 4;

  public Power(Translations translations, PlanetilerConfig config, Stats stats) {
  }


  @Override
  public void processAllOsm(SourceFeature feature, FeatureCollector features) {
    // basic implementation, more ideas here: https://github.com/ache051/openmaptiles/tree/power_facilities/layers/power
    if (feature.canBeLine() && feature.hasTag("power", "line")) {
      features.line(LAYER_NAME)
        .setBufferPixels(BUFFER_SIZE)
        .setMinZoom(14)
        .setAttr("class", "line");
    }
  }

  @Override
  public String name() {
    return LAYER_NAME;
  }
}
