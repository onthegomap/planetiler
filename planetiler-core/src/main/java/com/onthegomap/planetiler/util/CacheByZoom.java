package com.onthegomap.planetiler.util;

import com.onthegomap.planetiler.config.PlanetilerConfig;
import java.util.function.IntFunction;

/**
 * Caches a value that changes by integer zoom level to avoid recomputing it.
 *
 * @param <T> return type of the function
 */
public class CacheByZoom<T> {

  private final int minzoom;
  private final Object[] values;
  private final IntFunction<T> supplier;

  private CacheByZoom(int minzoom, int maxzoom, IntFunction<T> supplier) {
    this.minzoom = minzoom;
    values = new Object[maxzoom + 1 - minzoom];
    this.supplier = supplier;
  }

  /**
   * Returns a cache for {@code supplier} that can handle a min/max zoom range specified in {@code config}.
   *
   * @param config   min/max zoom range this can handle
   * @param supplier function that will be called with each zoom-level to get the value
   * @param <T>      return type of the function
   * @return a cache for {@code supplier} by zom
   */
  public static <T> CacheByZoom<T> create(PlanetilerConfig config, IntFunction<T> supplier) {
    return new CacheByZoom<>(0, PlanetilerConfig.MAX_MAXZOOM, supplier);
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
