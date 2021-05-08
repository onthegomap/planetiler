package com.onthegomap.flatmap;

public interface ZoomDependent<T> {

  T getValueAtZoom(int zoom);

  interface DoubleValue extends ZoomDependent<Double> {

    double getDoubleValueAtZoom(int zoom);

    @Override
    default Double getValueAtZoom(int zoom) {
      return getDoubleValueAtZoom(zoom);
    }
  }

  interface IntValue extends ZoomDependent<Integer> {

    int getIntValueAtZoom(int zoom);

    @Override
    default Integer getValueAtZoom(int zoom) {
      return getIntValueAtZoom(zoom);
    }
  }
}
