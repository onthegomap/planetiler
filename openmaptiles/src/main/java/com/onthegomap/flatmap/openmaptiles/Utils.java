package com.onthegomap.flatmap.openmaptiles;

public class Utils {

  public static <T> T coalesce(T a, T b) {
    return a != null ? a : b;
  }

  public static <T> T coalesce(T a, T b, T c) {
    return a != null ? a : b != null ? b : c;
  }

  public static <T> T coalesce(T a, T b, T c, T d) {
    return a != null ? a : b != null ? b : c != null ? d : d;
  }

  public static <T> T nullIf(T a, T nullValue) {
    return nullValue.equals(a) ? null : a;
  }

  public static Integer metersToFeet(Integer meters) {
    return meters != null ? (int) (meters * 3.2808399) : null;
  }
}
