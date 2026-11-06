package com.onthegomap.planetiler.archive;

import com.onthegomap.planetiler.config.PlanetilerConfig;
import com.onthegomap.planetiler.stats.Stats;
import java.util.function.Supplier;

/** Creates the {@link TileEncoder} for the tile format of a {@link PlanetilerConfig}. */
final class TileEncoders {

  private TileEncoders() {}

  /** Returns a factory of encoders for the tile format of {@code config}, to call once per encoding thread. */
  static Supplier<TileEncoder> factory(PlanetilerConfig config, Stats stats) {
    return switch (config.tileFormat()) {
      case MLT -> () -> new JavaMltTileEncoder(config, stats);
      case UNKNOWN, MVT -> MvtTileEncoder::new;
    };
  }
}
