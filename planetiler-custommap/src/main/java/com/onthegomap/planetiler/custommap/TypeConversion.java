package com.onthegomap.planetiler.custommap;

import com.onthegomap.planetiler.util.Parse;
import java.util.List;
import java.util.function.Function;

/**
 * Utility for convert between types in a forgiving way (parse strings to get a number, call toString to get a string,
 * etc.).
 */
public class TypeConversion {

  // convert() uses the first conversion from this list where:
  // - the input is a subclass of the first argument
  // - and expected output is equal to, or a superclass of the second argument
  // so put more specific conversions first, and general fallbacks last
  // NOTE: only does single-hop conversions, does NOT attempt to chain together multiple conversions
  private static final List<Converter<?, ?>> CONVERTERS = List.of(
    converter(Number.class, Double.class, Number::doubleValue),
    converter(Number.class, Integer.class, Number::intValue),
    converter(Number.class, Long.class, Number::longValue),

    converter(String.class, Double.class, Parse::parseDoubleOrNull),
    converter(String.class, Integer.class, Parse::parseIntOrNull),
    converter(String.class, Long.class, Parse::parseLongOrNull),

    converter(Integer.class, Boolean.class, n -> n != 0),
    converter(Long.class, Boolean.class, n -> n != 0),
    converter(Number.class, Boolean.class, n -> Math.abs(n.doubleValue()) > 2 * Double.MIN_VALUE),
    converter(String.class, Boolean.class, s -> Parse.bool(s.toLowerCase())),
    converter(Object.class, Boolean.class, Parse::bool),

    converter(Double.class, String.class, TypeConversion::doubleToString),
    converter(Object.class, String.class, Object::toString)
  );

  private TypeConversion() {}

  private static <I, O> Converter<I, O> converter(Class<I> in, Class<O> out, Function<I, O> fn) {
    return new Converter<>(in, out, fn);
  }

  /**
   * Attempts to coerce {@code in} to an instance {@code out} using the first registered conversion functions that
   * applies.
   *
   * @throws IllegalArgumentException if there is no available conversion
   */
  @SuppressWarnings("unchecked")
  public static <O> O convert(Object in, Class<O> out) {
    if (in == null || out.isInstance(in)) {
      return (O) in;
    }
    for (var converter : CONVERTERS) {
      if (converter.canConvertBetween(in.getClass(), out)) {
        return (O) converter.apply(in);
      }
    }
    throw new IllegalArgumentException(
      "No conversion from " + in.getClass().getSimpleName() + " to " + out.getSimpleName());
  }

  private static String doubleToString(Double d) {
    return d % 1 == 0 ? Long.toString(d.longValue()) : d.toString();
  }

  private record Converter<I, O> (Class<I> in, Class<O> out, Function<I, O> fn) implements Function<Object, O> {
    @Override
    public O apply(Object in) {
      @SuppressWarnings("unchecked") I converted = (I) in;
      try {
        return fn.apply(converted);
      } catch (NumberFormatException e) {
        return null;
      }
    }

    boolean canConvertTo(Class<?> clazz) {
      return clazz.isAssignableFrom(out);
    }

    boolean canConvertFrom(Class<?> clazz) {
      return in.isAssignableFrom(clazz);
    }

    boolean canConvertBetween(Class<?> from, Class<?> to) {
      return canConvertFrom(from) && canConvertTo(to);
    }
  }
}
