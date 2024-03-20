package com.onthegomap.planetiler.stream;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.onthegomap.planetiler.archive.TileArchiveMetadata;
import java.util.Arrays;
import java.util.Objects;

@JsonTypeInfo(
  use = JsonTypeInfo.Id.NAME,
  include = JsonTypeInfo.As.PROPERTY,
  property = "type")
@JsonSubTypes({
  @JsonSubTypes.Type(value = JsonStreamArchiveEntry.TileEntry.class, name = "tile"),
  @JsonSubTypes.Type(value = JsonStreamArchiveEntry.InitializationEntry.class, name = "initialization"),
  @JsonSubTypes.Type(value = JsonStreamArchiveEntry.FinishEntry.class, name = "finish")
})
sealed interface JsonStreamArchiveEntry {
  record TileEntry(int x, int y, int z, byte[] encodedData) implements JsonStreamArchiveEntry {

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + Arrays.hashCode(encodedData);
      result = prime * result + Objects.hash(x, y, z);
      return result;
    }

    @Override
    public boolean equals(Object obj) {
      return this == obj || (obj instanceof JsonStreamArchiveEntry.TileEntry tileEntry &&
        Arrays.equals(encodedData, tileEntry.encodedData) && x == tileEntry.x && y == tileEntry.y && z == tileEntry.z);
    }

    @Override
    public String toString() {
      return "TileEntry [x=" + x + ", y=" + y + ", z=" + z + ", encodedData=" + Arrays.toString(encodedData) + "]";
    }
  }

  record InitializationEntry() implements JsonStreamArchiveEntry {}


  record FinishEntry(TileArchiveMetadata metadata) implements JsonStreamArchiveEntry {}
}
