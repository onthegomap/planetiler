package com.onthegomap.planetiler.util;

import java.util.TreeMap;
import java.util.function.DoubleUnaryOperator;

public class Interpolator<T extends Interpolator<T>> implements DoubleUnaryOperator {
  private static final DoubleUnaryOperator IDENTITY = x -> x;

  private DoubleUnaryOperator transform = IDENTITY;
  private DoubleUnaryOperator reverseTransform = IDENTITY;
  private boolean clamp = false;
  private double defaultValue = Double.NaN;
  private final TreeMap<Double, Double> stops = new TreeMap<>();
  private TreeMap<Double, Double> forwardMap;
  private TreeMap<Double, Double> reverseMap;
  private double minKey = Double.POSITIVE_INFINITY;
  private double maxKey = Double.NEGATIVE_INFINITY;

  T setTransforms(DoubleUnaryOperator forward, DoubleUnaryOperator reverse) {
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
    var lo = stops.floorEntry(operand);
    var hi = stops.higherEntry(operand);
    if (lo == null) {
      if (hi == null) {
        return defaultValue;
      }
      lo = hi;
      hi = stops.higherEntry(lo.getKey());
      if (hi == null) {
        return lo.getValue();
      }
    } else if (hi == null) {
      hi = lo;
      lo = stops.lowerEntry(hi.getKey());
      if (lo == null) {
        return hi.getValue();
      }
    }
    double x = transform.applyAsDouble(operand);
    double x1 = transform.applyAsDouble(lo.getKey()),
      x2 = transform.applyAsDouble(hi.getKey()),
      y1 = lo.getValue(), y2 = hi.getValue();
    return (x - x1) / (x2 - x1) * (y2 - y1) + y1;
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
    minKey = Math.min(stop, minKey);
    maxKey = Math.max(stop, maxKey);
    stops.put(stop, value);
    return self();
  }

  private static class Inverted extends Interpolator<Inverted> {}

  public DoubleUnaryOperator invert() {
    var result = new Inverted().setTransforms(IDENTITY, IDENTITY);
    for (var entry : stops.entrySet()) {
      result.put(entry.getValue(), transform.applyAsDouble(entry.getKey()));
    }
    DoubleUnaryOperator retVal = result.andThen(reverseTransform);
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
