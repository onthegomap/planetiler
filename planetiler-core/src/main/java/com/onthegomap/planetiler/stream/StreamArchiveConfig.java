package com.onthegomap.planetiler.stream;

import com.onthegomap.planetiler.config.Arguments;
import com.onthegomap.planetiler.config.PlanetilerConfig;

public record StreamArchiveConfig(boolean appendToFile, Arguments moreOptions) {
  public StreamArchiveConfig(PlanetilerConfig planetilerConfig, Arguments moreOptions) {
    this(planetilerConfig.append(), moreOptions);
  }
}
