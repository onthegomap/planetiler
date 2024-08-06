package com.onthegomap.planetiler.util;

import com.onthegomap.planetiler.geo.GeoUtils;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.IntFunction;

/**
 * 一个在不同缩放级别返回不同值的接口。
 * <p>
 * {@link #apply(int)} 返回在指定缩放级别的值。
 */
public interface ZoomFunction<T> extends IntFunction<T> {

  /** 返回 {@code value} 当 {@code zoom >= min} 时，否则返回 null。 */
  static <T> ZoomFunction<T> minZoom(int min, T value) {
    return zoom -> zoom >= min ? value : null;
  }

  /** 返回 {@code value} 当缩放级别在 min 和 max 之间（包括），否则返回 null。 */
  static <T> ZoomFunction<T> zoomRange(int min, int max, T value) {
    return zoom -> zoom >= min && zoom <= max ? value : null;
  }

  /** 返回 {@code value} 当 {@code zoom <= max} 时，否则返回 null。 */
  static <T> ZoomFunction<T> maxZoom(int max, T value) {
    return zoom -> zoom <= max ? value : null;
  }

  /** 在指定缩放级别调用函数并返回 {@code defaultValue}，如果函数或结果为 null。 */
  static double applyAsDoubleOrElse(ZoomFunction<? extends Number> fn, int zoom, double defaultValue) {
    if (fn == null) {
      return defaultValue;
    }
    Number result = fn.apply(zoom);
    return result == null ? defaultValue : result.doubleValue();
  }

  /** 在指定缩放级别调用函数并返回 {@code defaultValue}，如果函数或结果为 null。 */
  static int applyAsIntOrElse(ZoomFunction<? extends Number> fn, int zoom, int defaultValue) {
    if (fn == null) {
      return defaultValue;
    }
    Number result = fn.apply(zoom);
    return result == null ? defaultValue : result.intValue();
  }

  /**
   * 返回一个缩放函数，该函数返回 {@code thresholds} 中的下一个更高的键的值，或者如果超过最大键则返回 {@code null}。
   */
  static <T> ZoomFunction<T> fromMaxZoomThresholds(Map<Integer, ? extends T> thresholds) {
    return fromMaxZoomThresholds(thresholds, null);
  }

  /**
   * 返回一个缩放函数，该函数返回 {@code thresholds} 中的下一个更高的键的值，或者 {@code defaultValue}。
   */
  static <T> ZoomFunction<T> fromMaxZoomThresholds(Map<Integer, ? extends T> thresholds, T defaultValue) {
    TreeMap<Integer, T> orderedMap = new TreeMap<>(thresholds);
    orderedMap.put(Integer.MAX_VALUE, defaultValue);
    return zoom -> orderedMap.ceilingEntry(zoom).getValue();
  }

  /**
   * 一个缩放函数，允许您设置返回的值为米，并在调用时返回在赤道上该米数有多少像素长。
   */
  class MeterToPixelThresholds implements ZoomFunction<Number> {

    private final TreeMap<Integer, Number> levels = new TreeMap<>();

    private MeterToPixelThresholds() {}

    /** 设置在 {@code zoom} 时返回的值，以米为单位。 */
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
