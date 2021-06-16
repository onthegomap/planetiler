package com.onthegomap.flatmap.openmaptiles;

import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;

public class Utils {

  public static <T> T coalesce(T a, T b) {
    return a != null ? a : b;
  }

  public static <T> T coalesce(T a, T b, T c) {
    return a != null ? a : b != null ? b : c;
  }

  public static <T> T coalesce(T a, T b, T c, T d) {
    return a != null ? a : b != null ? b : c != null ? c : d;
  }

  public static <T> T coalesceLazy(T a, Supplier<T> b) {
    return a != null ? a : b.get();
  }

  public static <T, U> T coalesceLazy(T a, Function<U, T> b, U arg) {
    return a != null ? a : b.apply(arg);
  }

  public static <T> T nullIf(T a, T nullValue) {
    return nullValue.equals(a) ? null : a;
  }

  public static Map<String, Object> elevationTags(int meters) {
    return Map.of(
      "ele", meters,
      "ele_ft", (int) Math.round(meters * 3.2808399)
    );
  }
}
