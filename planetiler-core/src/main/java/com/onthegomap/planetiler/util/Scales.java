package com.onthegomap.planetiler.util;

import com.carrotsearch.hppc.DoubleArrayList;
import java.util.ArrayList;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.DoubleFunction;
import java.util.function.DoubleUnaryOperator;
import java.util.function.Function;
import org.apache.commons.lang3.ArrayUtils;

public class Scales {
  private static final DoubleInterpolator INTERPOLATE_NUMERIC =
    (a, b) -> t -> a * (1 - t) + b * t;

  public interface Interpolator<V> extends BiFunction<V, V, DoubleFunction<V>> {}

  @FunctionalInterface
  public interface DoubleInterpolator extends Interpolator<Double> {

    DoubleUnaryOperator apply(double x, double y);

    @Override
    default DoubleFunction<Double> apply(Double x, Double y) {
      return apply(x.doubleValue(), y.doubleValue())::applyAsDouble;
    }
  }

  private interface Scale<T extends Scale<T, V>, V> extends DoubleFunction<V>, Function<Double, V> {

    @Override
    default V apply(Double x) {
      return apply(x.doubleValue());
    }

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

  private interface Threshold<T extends Threshold<T, V>, V> {

    T putBelow(double x, V y);

    T putAbove(V y);

    double invertMin(V x);

    double invertMax(V x);

    double[] invertExtent(V x);
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

    protected BaseContinuous(Interpolator<V> valueInterpolator, DoubleUnaryOperator transform) {
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

  public static class DoubleContinuous extends BaseContinuous<DoubleContinuous, Double>
    implements ContinuousDoubleScale<DoubleContinuous> {
    private final DoubleUnaryOperator reverseTransform;

    protected DoubleContinuous(Interpolator<Double> valueInterpolator, DoubleUnaryOperator transform,
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

    protected OtherContinuous(Interpolator<V> valueInterpolator, DoubleUnaryOperator transform) {
      super(valueInterpolator, transform);
    }
  }

  public static DoubleContinuous linear() {
    return new DoubleContinuous(INTERPOLATE_NUMERIC, BaseContinuous.IDENTITY, BaseContinuous.IDENTITY);
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

  public static DoubleContinuous log(double base) {
    return new DoubleContinuous(INTERPOLATE_NUMERIC, logFn(base), expFn(base));
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

  public static <V> OtherContinuous<V> log(double base, Interpolator<V> valueInterpolator) {
    return new OtherContinuous<>(valueInterpolator, logFn(base));
  }
}
