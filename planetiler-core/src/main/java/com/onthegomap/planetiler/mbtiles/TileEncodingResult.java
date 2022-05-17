package com.onthegomap.planetiler.mbtiles;

import com.onthegomap.planetiler.geo.TileCoord;
import java.util.Arrays;
import java.util.Objects;
import javax.annotation.Nullable;

public record TileEncodingResult(
  TileCoord coord,
  byte[] tileData,
  boolean memoized,
  /** will always be null in non-compact mode and might also be null in compact mode */
  @Nullable Integer tileDataHash
) {

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + Arrays.hashCode(tileData);
    result = prime * result + Objects.hash(coord, memoized, tileDataHash);
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
    return Objects.equals(coord, other.coord) && memoized == other.memoized &&
      Arrays.equals(tileData, other.tileData) && Objects.equals(tileDataHash, other.tileDataHash);
  }

  @Override
  public String toString() {
    return "TileEncodingResult [coord=" + coord + ", tileData=" + Arrays.toString(tileData) + ", memoized=" + memoized +
      ", tileDataHash=" + tileDataHash + "]";
  }

}
