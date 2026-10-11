package com.onthegomap.planetiler.util;

import com.carrotsearch.hppc.DoubleArrayList;
import java.util.ArrayList;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.DoubleFunction;
import java.util.function.DoubleUnaryOperator;
import org.apache.commons.lang3.ArrayUtils;

public class Scales {
  private static final Interpolator<Double> INTERPOLATE_NUMERIC =
    (a, b) -> t -> a * (1 - t) + b * t;

  public static <V> ThresholdScale<V> quantize(int min, int max, V minValue, V... values) {
    ThresholdScale<V> result = threshold(minValue);
    int n = values.length;
    for (int i = 0; i < n; i++) {
      result.putAbove(((i + 1d) * max - (i - n) * min) / (n + 1), values[i]);
    }
    return result;
  }

  public static <V> ThresholdScale<V> quantize(int min, int max, List<V> values) {
    ThresholdScale<V> result = threshold(values.getFirst());
    int n = values.size() - 1;
    int i = -1;
    while (++i < n) {
      result.putAbove(((i + 1d) * max - (i - n) * min) / (n + 1), values.get(i + 1));
    }
    return result;
  }


  public interface Interpolator<V> extends BiFunction<V, V, DoubleFunction<V>> {}

  private interface Scale<T extends Scale<T, V>, V> extends DoubleFunction<V> {

    T defaultValue(V defaultValue);

    @SuppressWarnings("unchecked")
    default T self() {
      return (T) this;
    }
  }

  private interface Continuous<T extends Continuous<T, V>, V> extends Scale<T, V> {

    T put(double x, V y);

    T clamp(boolean clamp);
  }

  private interface Threshold<T extends Threshold<T, V>, V> extends Scale<T, V> {

    T putAbove(double x, V y);

    double invertMin(V x);

    double invertMax(V x);

    Extent invertExtent(V x);
  }

  public record Extent(double min, double max) {}

  public static class ThresholdScale<V> implements Threshold<ThresholdScale<V>, V> {

    private V defaultValue = null;
    private final List<Double> domain = new ArrayList<>();
    private final List<V> range = new ArrayList<>();
    private double[] domainArray = null;

    private ThresholdScale(V minValue) {
      range.add(minValue);
    }

    @Override
    public ThresholdScale<V> putAbove(double x, V y) {
      domainArray = null;
      domain.add(x);
      range.add(y);
      return self();
    }

    @Override
    public double invertMin(V x) {
      int idx = range.indexOf(x);
      return idx < 0 ? Double.NaN : idx == 0 ? Double.NEGATIVE_INFINITY : domain.get(idx - 1);
    }

    @Override
    public double invertMax(V x) {
      int idx = range.indexOf(x);
      return idx < 0 ? Double.NaN : idx == range.size() - 1 ? Double.POSITIVE_INFINITY : domain.get(idx);
    }

    @Override
    public Extent invertExtent(V x) {
      int idx = range.indexOf(x);
      return new Extent(
        idx < 0 ? Double.NaN : idx == 0 ? Double.NEGATIVE_INFINITY : domain.get(idx - 1),
        idx < 0 ? Double.NaN : idx == range.size() - 1 ? Double.POSITIVE_INFINITY : domain.get(idx)
      );
    }

    @Override
    public ThresholdScale<V> defaultValue(V defaultValue) {
      this.defaultValue = defaultValue;
      return self();
    }

    @Override
    public V apply(double x) {
      if (domainArray == null) {
        domainArray = new double[domain.size()];
        int i = 0;
        for (Double d : domain) {
          domainArray[i++] = d;
        }
      }
      if (Double.isNaN(x)) {
        return defaultValue;
      }
      int idx = bisect(domainArray, x, 0, Math.min(domainArray.length, range.size() - 1));
      return range.get(idx);
    }
  }

  private interface ContinuousDoubleScale<T extends ContinuousDoubleScale<T>>
    extends Continuous<T, Double>, DoubleUnaryOperator {

    default T put(double x, double y) {
      return put(x, Double.valueOf(y));
    }

    @Override
    default double applyAsDouble(double operand) {
      return apply(operand);
    }

    DoubleUnaryOperator invert();
  }


  private static class BaseContinuous<T extends BaseContinuous<T, V>, V> implements Continuous<T, V> {
    static final DoubleUnaryOperator IDENTITY = x -> x;

    private final Interpolator<V> valueInterpolator;
    final DoubleUnaryOperator transform;
    boolean clamp = false;
    private V defaultValue;
    final DoubleArrayList domain = new DoubleArrayList();
    final List<V> range = new ArrayList<>();
    private DoubleFunction<V> fn;
    double minKey = Double.POSITIVE_INFINITY;
    double maxKey = Double.NEGATIVE_INFINITY;

    private BaseContinuous(Interpolator<V> valueInterpolator, DoubleUnaryOperator transform) {
      this.valueInterpolator = valueInterpolator;
      this.transform = transform;
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
        DoubleFunction<V> interpolate = valueInterpolator.apply(rlo, rhi);
        return x -> {
          double value = (x - dlo) / (dhi - dlo);
          return interpolate.apply(value);
        };
      }
    }

    private static DoubleUnaryOperator normalize(double a, double b) {
      double delta = b - a;
      return delta == 0 ? x -> 0.5 : Double.isNaN(delta) ? x -> Double.NaN : x -> (x - a) / delta;
    }

    @Override
    public T put(double x, V y) {
      fn = null;
      minKey = Math.min(x, minKey);
      maxKey = Math.max(x, maxKey);
      domain.add(x);
      range.add(y);
      return self();
    }

    @Override
    public T clamp(boolean clamp) {
      this.clamp = clamp;
      return self();
    }

    @Override
    public T defaultValue(V defaultValue) {
      this.defaultValue = defaultValue;
      return self();
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

  public static class DoubleContinuous extends BaseContinuous<DoubleContinuous, Double>
    implements ContinuousDoubleScale<DoubleContinuous> {
    private final DoubleUnaryOperator reverseTransform;

    private DoubleContinuous(Interpolator<Double> valueInterpolator, DoubleUnaryOperator transform,
      DoubleUnaryOperator reverseTransform) {
      super(valueInterpolator, transform);
      this.reverseTransform = reverseTransform;
    }

    @Override
    public DoubleUnaryOperator invert() {
      var result = linear();
      int j = Math.min(domain.size(), range.size());
      for (int i = 0; i < j; i++) {
        result.put(range.get(i).doubleValue(),
          transform.applyAsDouble(domain.get(i)));
      }
      DoubleUnaryOperator retVal =
        reverseTransform == IDENTITY ? result : x -> reverseTransform.applyAsDouble(result.apply(x));
      return clamp ? retVal.andThen(x -> Math.clamp(x, minKey, maxKey)) : retVal;
    }
  }

  public static class OtherContinuous<V> extends BaseContinuous<OtherContinuous<V>, V> {

    private OtherContinuous(Interpolator<V> valueInterpolator, DoubleUnaryOperator transform) {
      super(valueInterpolator, transform);
    }
  }

  public static DoubleContinuous linear() {
    return new DoubleContinuous(INTERPOLATE_NUMERIC, BaseContinuous.IDENTITY, BaseContinuous.IDENTITY);
  }

  public static DoubleContinuous linearDouble(Interpolator<Double> interp) {
    return new DoubleContinuous(interp, BaseContinuous.IDENTITY, BaseContinuous.IDENTITY);
  }

  public static <V> OtherContinuous<V> linear(Interpolator<V> valueInterpolator) {
    return new OtherContinuous<>(valueInterpolator, BaseContinuous.IDENTITY);
  }


  public static DoubleContinuous power(double exponent) {
    double inverse = 1d / exponent;
    return new DoubleContinuous(INTERPOLATE_NUMERIC,
      x -> Math.pow(x, exponent),
      y -> Math.pow(y, inverse)
    );
  }

  public static <V> OtherContinuous<V> power(double exponent, Interpolator<V> valueInterpolator) {
    return new OtherContinuous<>(valueInterpolator, x -> Math.pow(x, exponent));
  }

  public static DoubleContinuous sqrt() {
    return power(0.5);
  }

  public static <V> OtherContinuous<V> sqrt(Interpolator<V> valueInterpolator) {
    return power(0.5, valueInterpolator);
  }


  private static DoubleUnaryOperator logBase(double base) {
    double logBase = Math.log(base);
    return x -> Math.log(x) / logBase;
  }

  private static DoubleUnaryOperator expFn(double base) {
    return base == Math.E ? Math::exp :
      x -> Math.pow(base, x);
  }

  private static DoubleUnaryOperator logFn(double base) {
    return base == 10d ? Math::log10 :
      base == Math.E ? Math::log :
      logBase(base);
  }

  public static DoubleContinuous log(double base) {
    return new DoubleContinuous(INTERPOLATE_NUMERIC, logFn(base), expFn(base));
  }

  public static <V> OtherContinuous<V> log(double base, Interpolator<V> valueInterpolator) {
    return new OtherContinuous<>(valueInterpolator, logFn(base));
  }

  public static DoubleContinuous exponential(double base) {
    return new DoubleContinuous(INTERPOLATE_NUMERIC, expFn(base), logFn(base));
  }

  public static <V> OtherContinuous<V> exponential(double base, Interpolator<V> valueInterpolator) {
    return new OtherContinuous<>(valueInterpolator, expFn(base));
  }

  public static OtherContinuous<Double> bezier(double x1, double y1, double x2, double y2) {
    var bezier = new UnitBezier(x1, y1, x2, y2);
    return new OtherContinuous<>((a, b) -> {
      double diff = b - a;
      return t -> bezier.solve(t) * diff + a;
    }, BaseContinuous.IDENTITY);
  }


  public static <V> ThresholdScale<V> threshold(V minValue) {
    return new ThresholdScale<>(minValue);
  }
}
