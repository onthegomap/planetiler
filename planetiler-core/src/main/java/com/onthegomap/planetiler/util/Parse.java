package com.onthegomap.planetiler.util;

import java.util.Map;
import java.util.regex.Pattern;

/**
 * Utilities to parse values from strings.
 */
public class Parse {

  private Parse() {}

  private static final Pattern INT_SUBSTRING_PATTERN = Pattern.compile("^(-?\\d+)(\\D|$)");
  private static final Pattern TO_ROUND_INT_SUBSTRING_PATTERN = Pattern.compile("^(-?[\\d.]+)(\\D|$)");

  /** Returns {@code tag} as a long or null if invalid. */
  public static Long parseLongOrNull(Object tag) {
    try {
      return tag == null ? null : tag instanceof Number number ? number.longValue() : Long.parseLong(tag.toString());
    } catch (NumberFormatException e) {
      return null;
    }
  }

  /** Returns {@code tag} as a long or 0 if invalid. */
  public static long parseLong(Object tag) {
    try {
      return tag == null ? 0 : tag instanceof Number number ? number.longValue() : Long.parseLong(tag.toString());
    } catch (NumberFormatException e) {
      return 0;
    }
  }

  /** Returns {@code tag} as an integer or null if invalid, ignoring any non-numeric suffix. */
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

  /** Returns {@code tag} as a number rounded to the nearest integer or null if invalid. */
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

  /** Returns {@code tag} as an integer or null if invalid. */
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

  /** Returns {@code tag} parsed as a boolean. */
  public static boolean bool(Object tag) {
    return Imposm3Parsers.bool(tag);
  }

  /** Returns {@code tag} parsed as an integer where 1 is true, 0 is false. */
  public static int boolInt(Object tag) {
    return Imposm3Parsers.boolInt(tag);
  }

  /** Returns {@code tag} parsed as an integer where 1 is true, 0 is false. */
  public static int direction(Object input) {
    return Imposm3Parsers.direction(input);
  }

  /**
   * Returns {@code tag} for an OSM road based on the tags that are present. Bridges are above roads appear above
   * tunnels and major roads appear above minor.
   */
  public static int wayzorder(Map<String, Object> tags) {
    return Imposm3Parsers.wayzorder(tags);
  }

  /** Returns {@code tag} as a double or null if invalid. */
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

  /** Parses a value for {@code -Xmx/-Xms} jvm option like "100" or "500k" or "15g" */
  public static long jvmMemoryStringToBytes(String value) {
    value = value.strip();
    char lastChar = value.charAt(value.length() - 1);
    if (Character.isDigit(lastChar)) {
      return Long.parseLong(value);
    } else {
      long base = Long.parseLong(value.substring(0, value.length() - 1));
      lastChar = Character.toLowerCase(lastChar);
      return switch (lastChar) {
        case 'k' -> base * 1024L;
        case 'm' -> base * 1024L * 1024L;
        case 'g' -> base * 1024L * 1024L * 1024L;
        default -> throw new IllegalArgumentException("Unrecognized suffix: " + Character.toLowerCase(lastChar));
      };
    }
  }
}
