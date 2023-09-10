package com.onthegomap.planetiler.archive;

import com.onthegomap.planetiler.geo.TileCoord;
import java.util.Arrays;

public record Tile(TileCoord coord, byte[] bytes) implements Comparable<Tile> {

  @Override
  public boolean equals(Object o) {
    return (this == o) || (o instanceof Tile other && coord.equals(other.coord) && Arrays.equals(bytes, other.bytes));
  }

  @Override
  public int hashCode() {
    int result = coord.hashCode();
    result = 31 * result + Arrays.hashCode(bytes);
    return result;
  }

  @Override
  public String toString() {
    return "Tile{coord=" + coord + ", data=byte[" + bytes.length + "]}";
  }

  @Override
  public int compareTo(Tile o) {
    return coord.compareTo(o.coord);
  }
}
