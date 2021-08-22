package com.onthegomap.flatmap.collection;

import com.onthegomap.flatmap.config.CommonParams;
import com.onthegomap.flatmap.stats.Stats;
import com.onthegomap.flatmap.util.DiskBacked;
import com.onthegomap.flatmap.util.MemoryEstimator;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

public interface FeatureSort extends Iterable<FeatureSort.Entry>, DiskBacked, MemoryEstimator.HasEstimate {

  static FeatureSort newExternalMergeSort(Path tempDir, CommonParams config, Stats stats) {
    return new ExternalMergeSort(tempDir, config, stats);
  }

  static FeatureSort newExternalMergeSort(Path dir, int workers, int chunkSizeLimit, boolean gzip, CommonParams config,
    Stats stats) {
    return new ExternalMergeSort(dir, workers, chunkSizeLimit, gzip, config, stats);
  }

  static FeatureSort newInMemory() {
    List<Entry> list = new ArrayList<>();
    return new FeatureSort() {
      @Override
      public void sort() {
        list.sort(Comparator.naturalOrder());
      }

      @Override
      public long size() {
        return list.size();
      }

      @Override
      public void add(Entry newEntry) {
        list.add(newEntry);
      }

      @Override
      public long estimateMemoryUsageBytes() {
        return 0;
      }

      @Override
      public long bytesOnDisk() {
        return 0;
      }


      @Override
      public Iterator<Entry> iterator() {
        return list.iterator();
      }
    };
  }

  void sort();

  long size();

  default List<Entry> toList() {
    List<Entry> list = new ArrayList<>();
    for (Entry entry : this) {
      list.add(entry);
    }
    return list;
  }

  void add(Entry newEntry);

  record Entry(long sortKey, byte[] value) implements Comparable<Entry> {

    @Override
    public int compareTo(Entry o) {
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
