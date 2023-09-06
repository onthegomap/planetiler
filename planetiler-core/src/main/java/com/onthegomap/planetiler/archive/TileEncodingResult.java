package com.onthegomap.planetiler.archive;

import com.onthegomap.planetiler.geo.TileCoord;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.OptionalLong;

public record TileEncodingResult(
  TileCoord coord,
  byte[] tileData,
  int rawTileSize,
  /** will always be empty in non-compact mode and might also be empty in compact mode */
  OptionalLong tileDataHash,
  List<LayerStats> layerStats
) {
  public TileEncodingResult(
    TileCoord coord,
    byte[] tileData,
    OptionalLong tileDataHash
  ) {
    this(coord, tileData, 0, tileDataHash, List.of());
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + Arrays.hashCode(tileData);
    result = prime * result + Objects.hash(coord, tileDataHash, layerStats);
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    return this == obj || (obj instanceof TileEncodingResult other &&
      Objects.equals(coord, other.coord) &&
      Arrays.equals(tileData, other.tileData) &&
      Objects.equals(tileDataHash, other.tileDataHash) &&
      Objects.equals(layerStats, other.layerStats));
  }

  @Override
  public String toString() {
    return "TileEncodingResult [coord=" + coord + ", tileData=" + Arrays.toString(tileData) + ", tileDataHash=" +
      tileDataHash + ", layerStats=" + layerStats + "]";
  }

  public record LayerStats(
    String name,
    int features,
    int totalBytes,
    int attrBytes,
    int attrValues
  ) {}
}
