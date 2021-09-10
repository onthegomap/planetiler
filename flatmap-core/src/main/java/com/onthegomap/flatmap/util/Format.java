package com.onthegomap.flatmap.util;

import java.text.NumberFormat;
import java.time.Duration;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

/**
 * Utilities for formatting values as strings.
 */
public class Format {

  private Format() {
  }

  private static final NavigableMap<Long, String> STORAGE_SUFFIXES = new TreeMap<>(Map.ofEntries(
    Map.entry(1_000L, "k"),
    Map.entry(1_000_000L, "M"),
    Map.entry(1_000_000_000L, "G"),
    Map.entry(1_000_000_000_000L, "T"),
    Map.entry(1_000_000_000_000_000L, "P")
  ));
  private static final NavigableMap<Long, String> NUMERIC_SUFFIXES = new TreeMap<>(Map.ofEntries(
    Map.entry(1_000L, "k"),
    Map.entry(1_000_000L, "M"),
    Map.entry(1_000_000_000L, "B"),
    Map.entry(1_000_000_000_000L, "T"),
    Map.entry(1_000_000_000_000_000L, "Q")
  ));

  private static final NumberFormat pf = NumberFormat.getPercentInstance();
  private static final NumberFormat nf = NumberFormat.getNumberInstance();
  private static final NumberFormat intF = NumberFormat.getNumberInstance();

  static {
    pf.setMaximumFractionDigits(0);
    nf.setMaximumFractionDigits(1);
    intF.setMaximumFractionDigits(0);
  }

  public static String padRight(String str, int size) {
    StringBuilder strBuilder = new StringBuilder(str);
    while (strBuilder.length() < size) {
      strBuilder.append(" ");
    }
    return strBuilder.toString();
  }

  public static String padLeft(String str, int size) {
    StringBuilder strBuilder = new StringBuilder(str);
    while (strBuilder.length() < size) {
      strBuilder.insert(0, " ");
    }
    return strBuilder.toString();
  }

  /** Returns a number of bytes formatted like "123" "1.2k" "240M", etc. */
  public static String formatStorage(Number num, boolean pad) {
    return format(num, pad, STORAGE_SUFFIXES);
  }

  /** Returns a number formatted like "123" "1.2k" "2.5B", etc. */
  public static String formatNumeric(Number num, boolean pad) {
    return format(num, pad, NUMERIC_SUFFIXES);
  }

  private static String format(Number num, boolean pad, NavigableMap<Long, String> suffixes) {
    long value = num.longValue();
    double doubleValue = num.doubleValue();
    if (value < 0) {
      return padLeft("-", pad ? 4 : 0);
    } else if (doubleValue > 0 && doubleValue < 1) {
      return padLeft("<1", pad ? 4 : 0);
    } else if (value < 1000) {
      // 0-999
      return padLeft(Long.toString(value), pad ? 4 : 0);
    }

    Map.Entry<Long, String> e = suffixes.floorEntry(value);
    Long divideBy = e.getKey();
    String suffix = e.getValue();

    long truncated = value / (divideBy / 10);
    boolean hasDecimal = truncated < 100 && (truncated % 10 != 0);
    return padLeft(hasDecimal ? (truncated / 10d) + suffix : (truncated / 10) + suffix, pad ? 4 : 0);
  }

  /** Returns 0.0-1.0 as a "0%" - "100%" with no decimal points. */
  public static String formatPercent(double value) {
    return pf.format(value);
  }

  /** Returns a number formatted with 1 decimal point. */
  public static String formatDecimal(double value) {
    return nf.format(value);
  }

  /** Returns a number formatted with 0 decimal points. */
  public static String formatInteger(Number value) {
    return intF.format(value);
  }

  /** Returns a duration formatted as fractional seconds with 1 decimal point. */
  public static String formatSeconds(Duration duration) {
    double seconds = duration.toNanos() * 1d / Duration.ofSeconds(1).toNanos();
    return formatDecimal(seconds < 1 ? seconds : Math.round(seconds)) + "s";
  }
}
