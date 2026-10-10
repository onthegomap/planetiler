package com.onthegomap.planetiler.util;

import java.util.function.BiFunction;
import java.util.function.DoubleFunction;
import java.util.function.DoubleUnaryOperator;

public interface IInterpolator<T extends IInterpolator<T, V>, V> extends DoubleFunction<V> {
  T self();

  T put(double x, V y);


  interface ValueInterpolator<V> extends BiFunction<V, V, DoubleFunction<V>> {}
  interface Continuous<T extends Interpolator<T, Double> & Continuous<T>>
    extends IInterpolator<T, Double>, DoubleUnaryOperator {
    default DoubleUnaryOperator invert() {
      return Interpolator.invertIt(this.self());
    }

    default T put(double x, double y) {
      return put(x, Double.valueOf(y));
    }
  }
}
