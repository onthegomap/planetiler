package com.onthegomap.planetiler.util;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.function.ToLongFunction;

/**
 * Implements a best-effort 1-D bin packing using the
 * <a href="https://en.wikipedia.org/wiki/First-fit-decreasing_bin_packing#Other_variants">best-fit decreasing</a>
 * algorithm.
 */
public class BinPack {
  private BinPack() {}

  /**
   * Returns {@code items} grouped into an approximately minimum number of bins under {@code maxBinSize} according to
   * {@code getSize} function.
   */
  public static <T> List<List<T>> pack(Collection<T> items, long maxBinSize, ToLongFunction<T> getSize) {
    class Bin {
      long size = 0;
      final List<T> items = new ArrayList<>();
    }
    var descendingItems = items.stream().sorted(Comparator.comparingLong(getSize).reversed()).toList();
    List<Bin> bins = new ArrayList<>();
    for (var item : descendingItems) {
      long size = getSize.applyAsLong(item);
      var bestBin = bins.stream()
        .filter(b -> maxBinSize - b.size >= size)
        // Instead of using the first bin that this element fits in, use the "fullest" bin.
        // This makes the algorithm "best-fit decreasing" instead of "first-fit decreasing"
        .max(Comparator.comparingLong(bin -> bin.size));
      Bin bin;
      if (bestBin.isPresent()) {
        bin = bestBin.get();
      } else {
        bins.add(bin = new Bin());
      }
      bin.items.add(item);
      bin.size += size;
    }
    return bins.stream().map(bin -> bin.items).toList();
  }
}
