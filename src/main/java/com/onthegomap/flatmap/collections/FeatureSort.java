package com.onthegomap.flatmap.collections;

import com.onthegomap.flatmap.monitoring.Stats;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.jetbrains.annotations.NotNull;

public interface FeatureSort extends Iterable<FeatureSort.Entry> {

  static FeatureSort newExternalMergeSort(Path tempDir, int threads, Stats stats) {
    return new ExternalMergeSort(tempDir, threads, stats);
  }

  static FeatureSort newExternalMergeSort(Path dir, int workers, int chunkSizeLimit, Stats stats) {
    return new ExternalMergeSort(dir, workers, chunkSizeLimit, stats);
  }

  void sort();

  default List<Entry> toList() {
    List<Entry> list = new ArrayList<>();
    for (Entry entry : this) {
      list.add(entry);
    }
    return list;
  }

  void add(Entry newEntry);

  long getStorageSize();

  record Entry(long sortKey, byte[] value) implements Comparable<Entry> {

    @Override
    public int compareTo(@NotNull Entry o) {
      return Long.compare(sortKey, o.sortKey);
    }

    @Override
    public String toString() {
      return "MergeSort.Entry{" +
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

      Entry entry = (Entry) o;

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
}
