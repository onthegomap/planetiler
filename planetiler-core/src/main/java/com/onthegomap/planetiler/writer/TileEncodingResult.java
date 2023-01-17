package com.onthegomap.planetiler.writer;

import com.onthegomap.planetiler.geo.TileCoord;
import java.util.Arrays;
import java.util.Objects;
import java.util.OptionalLong;

public record TileEncodingResult(
  TileCoord coord,
  byte[] tileData,
  /** will always be empty in non-compact mode and might also be empty in compact mode */
  OptionalLong tileDataHash
) {

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + Arrays.hashCode(tileData);
    result = prime * result + Objects.hash(coord, tileDataHash);
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof TileEncodingResult)) {
      return false;
    }
    TileEncodingResult other = (TileEncodingResult) obj;
    return Objects.equals(coord, other.coord) && Arrays.equals(tileData, other.tileData) &&
      Objects.equals(tileDataHash, other.tileDataHash);
  }

  @Override
  public String toString() {
    return "TileEncodingResult [coord=" + coord + ", tileData=" + Arrays.toString(tileData) + ", tileDataHash=" +
      tileDataHash + "]";
  }

}
