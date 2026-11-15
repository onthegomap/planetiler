package com.onthegomap.planetiler.archive;

import com.onthegomap.planetiler.config.PlanetilerConfig;
import com.onthegomap.planetiler.stats.Stats;

class JavaMltTileEncoderTest extends MltTileEncoderTest {

  @Override
  TileEncoder encoder(PlanetilerConfig config, Stats stats) {
    return new JavaMltTileEncoder(config, stats);
  }
}
