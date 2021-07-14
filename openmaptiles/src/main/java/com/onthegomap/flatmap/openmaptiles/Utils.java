package com.onthegomap.flatmap.openmaptiles;

import com.onthegomap.flatmap.Parse;
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

  public static <T> T coalesce(T a, T b, T c, T d, T e) {
    return a != null ? a : b != null ? b : c != null ? c : d != null ? d : e;
  }

  public static <T> T coalesce(T a, T b, T c, T d, T e, T f) {
    return a != null ? a : b != null ? b : c != null ? c : d != null ? d : e != null ? e : f;
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

  public static String nullIfEmpty(String a) {
    return (a == null || a.isEmpty()) ? null : a;
  }

  public static boolean nullOrEmpty(Object a) {
    return a == null || a.toString().isEmpty();
  }

  public static Map<String, Object> elevationTags(int meters) {
    return Map.of(
      "ele", meters,
      "ele_ft", (int) Math.round(meters * 3.2808399)
    );
  }

  public static Map<String, Object> elevationTags(String meters) {
    Integer ele = Parse.parseIntSubstring(meters);
    return ele == null ? Map.of() : elevationTags(ele);
  }

  public static String brunnel(boolean isBridge, boolean isTunnel) {
    return brunnel(isBridge, isTunnel, false);
  }

  public static String brunnel(boolean isBridge, boolean isTunnel, boolean isFord) {
    return isBridge ? "bridge" : isTunnel ? "tunnel" : isFord ? "ford" : null;
  }
}
