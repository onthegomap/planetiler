package com.onthegomap.planetiler.basemap;

import com.onthegomap.planetiler.ForwardingProfile;
import com.onthegomap.planetiler.config.PlanetilerConfig;
import com.onthegomap.planetiler.stats.Stats;
import com.onthegomap.planetiler.util.Translations;

/** Interface for all vector tile layer implementations that {@link BasemapProfile} delegates to. */
public interface Layer extends
  ForwardingProfile.Handler,
  ForwardingProfile.HandlerForLayer {

  @FunctionalInterface
  interface Constructor {

    Layer create(Translations translations, PlanetilerConfig config, Stats stats);
  }
}
