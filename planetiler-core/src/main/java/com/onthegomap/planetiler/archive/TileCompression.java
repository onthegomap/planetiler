package com.onthegomap.planetiler.archive;

import java.util.Arrays;
import java.util.stream.Stream;

public enum TileCompression {

  NONE("none"),
  GZIP("gzip");

  private final String id;

  TileCompression(String id) {
    this.id = id;
  }

  public static TileCompression fromId(String id) {
    return Arrays.stream(TileCompression.values())
      .filter(tdc -> tdc.id().equals(id))
      .findFirst()
      .orElseThrow(() -> new IllegalArgumentException("invalid compression ID; expected one of " +
        Stream.of(TileCompression.values()).map(TileCompression::id).toList()));
  }

  public String id() {
    return id;
  }
}
