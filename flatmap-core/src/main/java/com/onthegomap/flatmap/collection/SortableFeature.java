package com.onthegomap.flatmap.collection;

import java.util.Arrays;

public record SortableFeature(long sortKey, byte[] value) implements Comparable<SortableFeature> {

  @Override
  public int compareTo(SortableFeature o) {
    return Long.compare(sortKey, o.sortKey);
  }

  @Override
  public String toString() {
    return "SortableFeature{" +
      "sortKey=" + sortKey +
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

    if (sortKey != entry.sortKey) {
      return false;
    }
    return Arrays.equals(value, entry.value);
  }

  @Override
  public int hashCode() {
    int result = (int) (sortKey ^ (sortKey >>> 32));
    result = 31 * result + Arrays.hashCode(value);
    return result;
  }
}
