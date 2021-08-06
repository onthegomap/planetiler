package com.onthegomap.flatmap.util;

import com.onthegomap.flatmap.config.CommonParams;
import java.util.function.IntFunction;

public class CacheByZoom<T> {

  private final int minzoom;
  private final Object[] values;
  private final IntFunction<T> supplier;

  private CacheByZoom(int minzoom, int maxzoom, IntFunction<T> supplier) {
    this.minzoom = minzoom;
    values = new Object[maxzoom + 1 - minzoom];
    this.supplier = supplier;
  }

  public static <T> CacheByZoom<T> create(CommonParams params, IntFunction<T> supplier) {
    return new CacheByZoom<>(params.minzoom(), params.maxzoom(), supplier);
  }

  public T get(int zoom) {
    @SuppressWarnings("unchecked") T[] casted = (T[]) values;
    int off = zoom - minzoom;
    if (values[off] != null) {
      return casted[off];
    }
    return casted[off] = supplier.apply(zoom);
  }
}
