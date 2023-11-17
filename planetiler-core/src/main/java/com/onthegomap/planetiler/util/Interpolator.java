package com.onthegomap.planetiler.util;

import com.carrotsearch.hppc.DoubleArrayList;
import java.util.function.DoubleUnaryOperator;
import org.apache.commons.lang3.ArrayUtils;

public class Interpolator<T extends Interpolator<T>> implements DoubleUnaryOperator {
  private static final DoubleUnaryOperator IDENTITY = x -> x;

  private DoubleUnaryOperator transform = IDENTITY;
  private DoubleUnaryOperator reverseTransform = IDENTITY;
  private boolean clamp = false;
  private double defaultValue = Double.NaN;
  private final DoubleArrayList domain = new DoubleArrayList();
  private final DoubleArrayList range = new DoubleArrayList();
  private DoubleUnaryOperator fn;
  private double minKey = Double.POSITIVE_INFINITY;
  private double maxKey = Double.NEGATIVE_INFINITY;

  T setTransforms(DoubleUnaryOperator forward, DoubleUnaryOperator reverse) {
    fn = null;
    this.transform = forward;
    this.reverseTransform = reverse;
    return self();
  }

  @Override
  public double applyAsDouble(double operand) {
    if (clamp) {
      operand = Math.clamp(operand, minKey, maxKey);
    }
    if (Double.isNaN(operand)) {
      return defaultValue;
    }
    if (fn == null) {
      fn = rescale();
    }
    return fn.applyAsDouble(transform.applyAsDouble(operand));
  }

  private DoubleUnaryOperator rescale() {
    if (domain.size() > 2) {
      int j = Math.min(domain.size(), range.size()) - 1;
      DoubleUnaryOperator[] d = new DoubleUnaryOperator[j];
      DoubleUnaryOperator[] r = new DoubleUnaryOperator[j];
      int i = -1;

      double[] domainItems = new double[domain.size()];
      for (int k = 0; k < domainItems.length; k++) {
        domainItems[k] = transform.applyAsDouble(domain.get(k));
      }
      double[] rangeItems = range.toArray();

      // Reverse descending domains.
      if (domainItems[j] < domainItems[0]) {
        ArrayUtils.reverse(domainItems);
        ArrayUtils.reverse(rangeItems);
      }

      while (++i < j) {
        d[i] = normalize(domainItems[i], domainItems[i + 1]);
        r[i] = interpolate(rangeItems[i], rangeItems[i + 1]);
      }

      return x -> {
        int ii = bisect(domainItems, x, 1, j) - 1;
        return r[ii].applyAsDouble(d[ii].applyAsDouble(x));
      };
    } else {
      double d0 = transform.applyAsDouble(domain.get(0)), d1 = transform.applyAsDouble(domain.get(1)),
        r0 = range.get(0), r1 = range.get(1);
      boolean reverse = d1 < d0;
      final double dlo = reverse ? d1 : d0;
      final double dhi = reverse ? d0 : d1;
      final double rlo = reverse ? r1 : r0;
      final double rhi = reverse ? r0 : r1;
      double delta = dhi - dlo;
      return x -> {
        double t = delta == 0 ? 0.5 : Double.isNaN(delta) ? Double.NaN : (x - dlo) / delta;
        return rlo * (1 - t) + rhi * t;
      };
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

  public T defaultValue(double value) {
    this.defaultValue = value;
    return self();
  }

  public T put(double stop, double value) {
    fn = null;
    minKey = Math.min(stop, minKey);
    maxKey = Math.max(stop, maxKey);
    domain.add(stop);
    range.add(value);
    return self();
  }

  private static class Inverted extends Interpolator<Inverted> {}

  public DoubleUnaryOperator invert() {
    Interpolator<?> result = new Inverted();
    result.domain.addAll(range);
    for (int i = 0; i < domain.size(); i++) {
      result.range.add(transform.applyAsDouble(domain.get(i)));
    }
    DoubleUnaryOperator retVal = reverseTransform == IDENTITY ? result : result.andThen(reverseTransform);
    return clamp ? retVal.andThen(x -> Math.clamp(x, minKey, maxKey)) : retVal;
  }

  public static class Power extends Interpolator<Power> {
    public Power exponent(double exponent) {
      double inverse = 1d / exponent;
      return setTransforms(x -> Math.pow(x, exponent), y -> Math.pow(y, inverse));
    }
  }

  public static class Log extends Interpolator<Log> {

    private static DoubleUnaryOperator log(double base) {
      double logBase = Math.log(base);
      return x -> Math.log(x) / logBase;
    }

    public Log base(double base) {
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

  public static class Linear extends Interpolator<Linear> {}

  public static Linear linear() {
    return new Linear();
  }

  public static Log log() {
    return new Log().base(10);
  }

  public static Power power() {
    return new Power().exponent(1);
  }

  public static Power sqrt() {
    return power().exponent(0.5);
  }
}
