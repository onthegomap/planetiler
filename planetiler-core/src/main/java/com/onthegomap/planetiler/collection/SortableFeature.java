package com.onthegomap.planetiler.collection;

import java.util.Arrays;

public record SortableFeature(long key, byte[] value) implements Comparable<SortableFeature> {

  @Override
  public int compareTo(SortableFeature o) {
    return Long.compare(key, o.key);
  }

  @Override
  public String toString() {
    return "SortableFeature{" +
      "key=" + key +
      ", value=" + Arrays.toString(value) +
      '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    SortableFeature entry = (SortableFeature) o;

    if (key != entry.key) {
      return false;
    }
    return Arrays.equals(value, entry.value);
  }

  @Override
  public int hashCode() {
    int result = (int) (key ^ (key >>> 32));
    result = 31 * result + Arrays.hashCode(value);
    return result;
  }
}
