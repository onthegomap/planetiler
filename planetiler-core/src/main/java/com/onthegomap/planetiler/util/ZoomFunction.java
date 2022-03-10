package com.onthegomap.planetiler.util;

import com.onthegomap.planetiler.geo.GeoUtils;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.IntFunction;

/**
 * A value that changes by zoom level.
 * <p>
 * {@link #apply(int)} returns the value at the zoom level.
 */
public interface ZoomFunction<T> extends IntFunction<T> {

  /** Returns {@code value} when {@code zom >= min}, and null otherwise. */
  static <T> ZoomFunction<T> minZoom(int min, T value) {
    return zoom -> zoom >= min ? value : null;
  }

  /** Returns {@code value} when zoom is between min and max inclusive, null otherwise. */
  static <T> ZoomFunction<T> zoomRange(int min, int max, T value) {
    return zoom -> zoom >= min && zoom <= max ? value : null;
  }

  /** Returns {@code value} when {@code zoom <= max}, and null otherwise. */
  static <T> ZoomFunction<T> maxZoom(int max, T value) {
    return zoom -> zoom <= max ? value : null;
  }

  /** Invoke a function at a zoom level and returns {@code defaultValue} if the function or result were null. */
  static double applyAsDoubleOrElse(ZoomFunction<? extends Number> fn, int zoom, double defaultValue) {
    if (fn == null) {
      return defaultValue;
    }
    Number result = fn.apply(zoom);
    return result == null ? defaultValue : result.doubleValue();
  }

  /** Invoke a function at a zoom level and returns {@code defaultValue} if the function or result were null. */
  static int applyAsIntOrElse(ZoomFunction<? extends Number> fn, int zoom, int defaultValue) {
    if (fn == null) {
      return defaultValue;
    }
    Number result = fn.apply(zoom);
    return result == null ? defaultValue : result.intValue();
  }

  /**
   * Returns a zoom function that returns the value from the next higher key in {@code thresholds} or {@code null} if
   * over the max key.
   */
  static <T> ZoomFunction<T> fromMaxZoomThresholds(Map<Integer, ? extends T> thresholds) {
    return fromMaxZoomThresholds(thresholds, null);
  }

  /**
   * Returns a zoom function that returns the value from the next higher key in {@code thresholds} or {@code
   * defaultValue}.
   */
  static <T> ZoomFunction<T> fromMaxZoomThresholds(Map<Integer, ? extends T> thresholds, T defaultValue) {
    TreeMap<Integer, T> orderedMap = new TreeMap<>(thresholds);
    orderedMap.put(Integer.MAX_VALUE, defaultValue);
    return zoom -> orderedMap.ceilingEntry(zoom).getValue();
  }

  /**
   * A zoom function that lets you set the value to return for a zoom level in meters and when called, it returns how
   * many pixels long that number of meters is at the equator.
   */
  class MeterToPixelThresholds implements ZoomFunction<Number> {

    private final TreeMap<Integer, Number> levels = new TreeMap<>();

    private MeterToPixelThresholds() {}

    /** Sets the value to return at {@code zoom} in meters. */
    public MeterToPixelThresholds put(int zoom, double meters) {
      levels.put(zoom, GeoUtils.metersToPixelAtEquator(zoom, meters));
      return this;
    }

    @Override
    public Number apply(int value) {
      return levels.get(value);
    }
  }

  static MeterToPixelThresholds meterThresholds() {
    return new MeterToPixelThresholds();
  }
}
