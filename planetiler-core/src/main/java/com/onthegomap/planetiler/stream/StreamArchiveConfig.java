package com.onthegomap.planetiler.stream;

import com.onthegomap.planetiler.config.Arguments;
import com.onthegomap.planetiler.config.CommonConfigs;

public record StreamArchiveConfig(boolean appendToFile, Arguments formatOptions) {
  public StreamArchiveConfig(Arguments baseArguments, Arguments formatOptions) {
    this(CommonConfigs.appendToArchive(baseArguments), formatOptions);
  }
}
