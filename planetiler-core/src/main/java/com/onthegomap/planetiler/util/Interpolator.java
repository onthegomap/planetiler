package com.onthegomap.planetiler.util;

import com.carrotsearch.hppc.DoubleArrayList;
import java.util.ArrayList;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.DoubleFunction;
import java.util.function.DoubleUnaryOperator;
import org.apache.commons.lang3.ArrayUtils;

public class Interpolator<T extends Interpolator<T, V>, V> implements DoubleFunction<V> {
  private static final ValueInterpolator<Double> INTERPOLATE_NUMERIC =
    (a, b) -> t -> a * (1 - t) + b * t;
  private static final DoubleUnaryOperator IDENTITY = x -> x;
  private final ValueInterpolator<V> valueInterpolator;

  private DoubleUnaryOperator transform = IDENTITY;
  private DoubleUnaryOperator reverseTransform = IDENTITY;
  private boolean clamp = false;
  private V defaultValue;
  private final DoubleArrayList domain = new DoubleArrayList();
  private final List<V> range = new ArrayList<>();
  private DoubleFunction<V> fn;
  private double minKey = Double.POSITIVE_INFINITY;
  private double maxKey = Double.NEGATIVE_INFINITY;

  protected Interpolator(ValueInterpolator<V> valueInterpolator) {
    this.valueInterpolator = valueInterpolator;
  }

  T setTransforms(DoubleUnaryOperator forward, DoubleUnaryOperator reverse) {
    fn = null;
    this.transform = forward;
    this.reverseTransform = reverse;
    return self();
  }

  @Override
  public V apply(double operand) {
    if (clamp) {
      operand = Math.clamp(operand, minKey, maxKey);
    }
    if (Double.isNaN(operand)) {
      return defaultValue;
    }
    if (fn == null) {
      fn = rescale();
    }
    return fn.apply(transform.applyAsDouble(operand));
  }

  public double applyAsDouble(double operand) {
    return apply(operand) instanceof Number n ? n.doubleValue() : Double.NaN;
  }

  private DoubleFunction<V> rescale() {
    if (domain.size() > 2) {
      int j = Math.min(domain.size(), range.size()) - 1;
      DoubleUnaryOperator[] d = new DoubleUnaryOperator[j];
      DoubleFunction<V>[] r = new DoubleFunction[j];
      int i = -1;

      double[] domainItems = new double[domain.size()];
      for (int k = 0; k < domainItems.length; k++) {
        domainItems[k] = transform.applyAsDouble(domain.get(k));
      }
      List<V> rangeItems = domainItems[j] < domainItems[0] ? range.reversed() : range;

      // Reverse descending domains.
      if (domainItems[j] < domainItems[0]) {
        ArrayUtils.reverse(domainItems);
      }

      while (++i < j) {
        d[i] = normalize(domainItems[i], domainItems[i + 1]);
        r[i] = valueInterpolator.apply(rangeItems.get(i), rangeItems.get(i + 1));
      }

      return x -> {
        int ii = bisect(domainItems, x, 1, j) - 1;
        return r[ii].apply(d[ii].applyAsDouble(x));
      };
    } else {
      double d0 = transform.applyAsDouble(domain.get(0)), d1 = transform.applyAsDouble(domain.get(1));
      V r0 = range.get(0), r1 = range.get(1);
      boolean reverse = d1 < d0;
      final double dlo = reverse ? d1 : d0;
      final double dhi = reverse ? d0 : d1;
      final V rlo = reverse ? r1 : r0;
      final V rhi = reverse ? r0 : r1;
      DoubleUnaryOperator normalize = normalize(dlo, dhi);
      DoubleFunction<V> interpolate = valueInterpolator.apply(rlo, rhi);
      return x -> interpolate.apply(normalize.applyAsDouble(x));
    }
  }

  private static int bisect(double[] a, double x, int lo, int hi) {
    if (lo < hi) {
      do {
        int mid = (lo + hi) >>> 1;
        if (a[mid] <= x)
          lo = mid + 1;
        else
          hi = mid;
      } while (lo < hi);
    }
    return lo;
  }

  private static DoubleUnaryOperator interpolate(double a, double b) {
    return t -> a * (1 - t) + b * t;
  }

  private static DoubleUnaryOperator normalize(double a, double b) {
    double delta = b - a;
    return delta == 0 ? x -> 0.5 : Double.isNaN(delta) ? x -> Double.NaN : x -> (x - a) / delta;
  }

  @SuppressWarnings("unchecked")
  private T self() {
    return (T) this;
  }

  public T clamp(boolean clamp) {
    this.clamp = clamp;
    return self();
  }

  public T defaultValue(V value) {
    this.defaultValue = value;
    return self();
  }

  public T put(double stop, V value) {
    fn = null;
    minKey = Math.min(stop, minKey);
    maxKey = Math.max(stop, maxKey);
    domain.add(stop);
    range.add(value);
    return self();
  }

  // TODO
  //  private static class Inverted extends Interpolator<Inverted, Double> {}

  public DoubleUnaryOperator invert() {
    var result = linear();
    int j = Math.min(domain.size(), range.size());
    for (int i = 0; i < j; i++) {
      result.put((Double) range.get(i), transform.applyAsDouble(domain.get(i)));
    }
    DoubleUnaryOperator retVal =
      reverseTransform == IDENTITY ? result::applyAsDouble : x -> reverseTransform.applyAsDouble(result.apply(x));
    return clamp ? retVal.andThen(x -> Math.clamp(x, minKey, maxKey)) : retVal;
  }

  public static class Power<V> extends Interpolator<Power<V>, V> {

    public Power(ValueInterpolator<V> valueInterpolator) {
      super(valueInterpolator);
    }

    public Power<V> exponent(double exponent) {
      double inverse = 1d / exponent;
      return setTransforms(x -> Math.pow(x, exponent), y -> Math.pow(y, inverse));
    }
  }

  public static class Log<V> extends Interpolator<Log<V>, V> {

    public Log(ValueInterpolator<V> valueInterpolator) {
      super(valueInterpolator);
    }

    private static DoubleUnaryOperator log(double base) {
      double logBase = Math.log(base);
      return x -> Math.log(x) / logBase;
    }

    public Log<V> base(double base) {
      DoubleUnaryOperator forward =
        base == 10d ? Math::log10 :
          base == Math.E ? Math::log :
          log(base);
      DoubleUnaryOperator reverse =
        base == Math.E ? Math::exp :
          x -> Math.pow(base, x);
      return setTransforms(forward, reverse);
    }
  }

  public static class Linear<V> extends Interpolator<Linear<V>, V> {

    public Linear(ValueInterpolator<V> valueInterpolator) {
      super(valueInterpolator);
    }
  }

  public static <V> Linear<V> linear(ValueInterpolator<V> valueInterpolator) {
    return new Linear<>(valueInterpolator);
  }

  public static Linear<Double> linear() {
    return new Linear<>(INTERPOLATE_NUMERIC);
  }

  public static <V> Log<V> log(ValueInterpolator<V> valueInterpolator) {
    return new Log<>(valueInterpolator).base(10);
  }

  public static Log<Double> log() {
    return log(INTERPOLATE_NUMERIC).base(10);
  }

  public static <V> Power<V> power(ValueInterpolator<V> valueInterpolator) {
    return new Power<>(valueInterpolator).exponent(1);
  }

  public static Power<Double> power() {
    return new Power<>(INTERPOLATE_NUMERIC).exponent(1);
  }

  public static Power<Double> sqrt() {
    return power(INTERPOLATE_NUMERIC).exponent(0.5);
  }

  public interface ValueInterpolator<V> extends BiFunction<V, V, DoubleFunction<V>> {}
}
