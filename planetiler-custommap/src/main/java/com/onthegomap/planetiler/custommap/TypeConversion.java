package com.onthegomap.planetiler.custommap;

import com.onthegomap.planetiler.util.Parse;
import java.util.List;
import java.util.function.Function;

public class TypeConversion {

  private static final List<Converter<?, ?>> CONVERTERS = List.of(
    converter(Number.class, Double.class, Number::doubleValue),
    converter(Number.class, Integer.class, Number::intValue),
    converter(Number.class, Long.class, Number::longValue),

    converter(String.class, Double.class, Double::parseDouble),
    converter(String.class, Integer.class, Integer::parseInt),
    converter(String.class, Long.class, Long::parseLong),

    converter(Integer.class, Boolean.class, n -> n != 0),
    converter(Long.class, Boolean.class, n -> n != 0),
    converter(Number.class, Boolean.class, n -> Math.abs(n.doubleValue()) > 2 * Double.MIN_VALUE),
    converter(String.class, Boolean.class, s -> Parse.bool(s.toLowerCase())),

    converter(Double.class, String.class, TypeConversion::doubleToString),
    converter(Object.class, String.class, Object::toString)
  );

  private TypeConversion() {}

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

  private static <I, O> Converter<I, O> converter(Class<I> in, Class<O> out, Function<I, O> fn) {
    return new Converter<>(in, out, fn);
  }

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
}
