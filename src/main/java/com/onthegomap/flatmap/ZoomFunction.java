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

//  static ZoomFunction ranges(Range... ranges) {
//  }

//  static OfDouble doubleRanges(double defaultValue, DoubleRange... ranges) {
//  }
//
//  static OfInt intRanges(int defaultValue, IntRange... ranges) {
//  }
//
//  static ZoomFunction maxZoom(int max, Object value) {
//  }
//
//  static OfDouble doubleThreshold(double valueBelowThreshold, int threshold, double valueAtOrAboveThreshold) {
//  }
//
//  static OfInt intThreshold(int valueBelowThreshold, int threshold, int valueAtOrAboveThreshold) {
//  }
//
//  static OfDouble metersToPixelsAtEquator(int i, DoubleRange doubleRange, DoubleRange doubleRange1,
//    DoubleRange doubleRange2) {
//  }
//
//  record Range(int min, int max, Object value) {
//
//  }
//
//  static Range range(int min, int max, Object value) {
//    return new Range(min, max, value);
//  }
//
//  record DoubleRange(int min, int max, double value) {
//
//  }
//
//  static DoubleRange doubleRange(int min, int max, double value) {
//    return new DoubleRange(min, max, value);
//  }
//
//  record IntRange(int min, int max, int value) {
//
//  }
//
//  static IntRange intRange(int min, int max, int value) {
//    return new IntRange(min, max, value);
//  }

//  interface OfDouble extends IntToDoubleFunction, ZoomFunction {
//
//    @Override
//    default Double apply(int value) {
//      return applyAsDouble(value);
//    }
//
//    default OfDouble metersToPixelsAtEquator() {
//      return (int i) -> applyAsDouble(i) / GeoUtils.metersPerPixelAtEquator(i);
//    }
//  }
//
//  interface OfInt extends IntUnaryOperator, ZoomFunction {
//
//    @Override
//    default Integer apply(int value) {
//      return applyAsInt(value);
//    }
//  }

}
