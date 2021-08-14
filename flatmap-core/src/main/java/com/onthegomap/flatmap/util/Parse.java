package com.onthegomap.flatmap.util;

import java.util.Map;
import java.util.regex.Pattern;

public class Parse {

  public static Long parseLongOrNull(Object tag) {
    try {
      return tag == null ? null : tag instanceof Number number ? number.longValue() : Long.parseLong(tag.toString());
    } catch (NumberFormatException e) {
      return null;
    }
  }

  public static long parseLong(Object tag) {
    try {
      return tag == null ? 0 : tag instanceof Number number ? number.longValue() : Long.parseLong(tag.toString());
    } catch (NumberFormatException e) {
      return 0;
    }
  }

  private static final Pattern INT_SUBSTRING_PATTERN = Pattern.compile("^(-?\\d+)(\\D|$)");

  public static Integer parseIntSubstring(String tag) {
    if (tag == null) {
      return null;
    }
    try {
      var matcher = INT_SUBSTRING_PATTERN.matcher(tag);
      return matcher.find() ? Integer.parseInt(matcher.group(1)) : null;
    } catch (NumberFormatException e) {
      return null;
    }
  }

  private static final Pattern TO_ROUND_INT_SUBSTRING_PATTERN = Pattern.compile("^(-?[\\d.]+)(\\D|$)");

  public static Integer parseRoundInt(Object tag) {
    if (tag == null) {
      return null;
    }
    try {
      var matcher = TO_ROUND_INT_SUBSTRING_PATTERN.matcher(tag.toString());
      return matcher.find() ? Math.round(Float.parseFloat(matcher.group(1))) : null;
    } catch (NumberFormatException e) {
      return null;
    }
  }

  public static Integer parseIntOrNull(Object tag) {
    if (tag instanceof Number num) {
      return num.intValue();
    }
    if (!(tag instanceof String)) {
      return null;
    }
    try {
      return Integer.parseInt(tag.toString());
    } catch (NumberFormatException e) {
      return null;
    }
  }

  public static boolean bool(Object tag) {
    return Imposm3Parsers.bool(tag);
  }

  public static int boolInt(Object tag) {
    return Imposm3Parsers.boolInt(tag);
  }

  public static int direction(Object input) {
    return Imposm3Parsers.direction(input);
  }

  public static int wayzorder(Map<String, Object> tags) {
    return Imposm3Parsers.wayzorder(tags);
  }

  public static Double parseDoubleOrNull(Object value) {
    if (value instanceof Number num) {
      return num.doubleValue();
    }
    if (value == null) {
      return null;
    }
    try {
      return Double.parseDouble(value.toString());
    } catch (NumberFormatException e) {
      return null;
    }
  }
}
