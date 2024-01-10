package com.onthegomap.planetiler.collection;

import java.util.Arrays;

public record SortableFeature(@Override long key, byte[] value) implements Comparable<SortableFeature>, HasLongSortKey {

  @Override
  public int compareTo(SortableFeature o) {
    int cmp = Long.compare(key, o.key);
    if (cmp == 0) {
      cmp = Arrays.compareUnsigned(value, o.value);
    }
    return cmp;
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
