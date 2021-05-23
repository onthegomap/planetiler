package com.onthegomap.flatmap;

import com.onthegomap.flatmap.geo.GeoUtils;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.IntFunction;

public interface ZoomFunction<T> extends IntFunction<T> {

  static <T> ZoomFunction<T> minZoom(int min, T value) {
    return zoom -> zoom >= min ? value : null;
  }

  static <T> ZoomFunction<T> zoomRange(int min, int max, T value) {
    return zoom -> zoom >= min && zoom <= max ? value : null;
  }

  static <T> ZoomFunction<T> maxZoom(int max, T value) {
    return zoom -> zoom <= max ? value : null;
  }

  static double applyAsDoubleOrElse(ZoomFunction<? extends Number> fn, int zoom, double defaultValue) {
    if (fn == null) {
      return defaultValue;
    }
    Number result = fn.apply(zoom);
    return result == null ? defaultValue : result.doubleValue();
  }

  static int applyAsIntOrElse(ZoomFunction<? extends Number> fn, int zoom, int defaultValue) {
    if (fn == null) {
      return defaultValue;
    }
    Number result = fn.apply(zoom);
    return result == null ? defaultValue : result.intValue();
  }

  static <T> ZoomFunction<T> fromMaxZoomThresholds(Map<Integer, ? extends T> thresholds) {
    return fromMaxZoomThresholds(thresholds, null);
  }

  static <T> ZoomFunction<T> fromMaxZoomThresholds(Map<Integer, ? extends T> thresholds, T defaultValue) {
    TreeMap<Integer, T> orderedMap = new TreeMap<>(thresholds);
    orderedMap.put(Integer.MAX_VALUE, defaultValue);
    return zoom -> orderedMap.ceilingEntry(zoom).getValue();
  }

  static <T> ZoomFunction<T> constant(T value) {
    return zoom -> value;
  }

  static ZoomLevelFunction<Number, Double> metersToPixelsAtEquator() {
    return (zoom, last) -> last.doubleValue() / GeoUtils.metersPerPixelAtEquator(zoom);
  }

  default <U> ZoomFunction<U> andThen(ZoomLevelFunction<? super T, ? extends U> next) {
    return zoom -> {
      T last = apply(zoom);
      return last == null ? null : next.apply(zoom, last);
    };
  }

  default ZoomFunction<T> withDefault(T defaultValue) {
    return zoom -> {
      T last = apply(zoom);
      return last == null ? defaultValue : last;
    };
  }

  @FunctionalInterface
  interface ZoomLevelFunction<T, U> {

    U apply(int zoom, T t);
  }
}
