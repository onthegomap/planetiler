package com.onthegomap.flatmap;

import java.text.NumberFormat;
import java.time.Duration;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

public class Format {

  private static final NavigableMap<Long, String> STORAGE_SUFFIXES = new TreeMap<>(Map.ofEntries(
    Map.entry(1_000L, "kB"),
    Map.entry(1_000_000L, "MB"),
    Map.entry(1_000_000_000L, "GB"),
    Map.entry(1_000_000_000_000L, "TB"),
    Map.entry(1_000_000_000_000_000L, "PB")
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

  public static String formatStorage(Number num, boolean pad) {
    return format(num, pad, STORAGE_SUFFIXES);
  }

  public static String formatNumeric(Number num, boolean pad) {
    return format(num, pad, NUMERIC_SUFFIXES);
  }

  public static String format(Number num, boolean pad, NavigableMap<Long, String> suffixes) {
    long value = num.longValue();
    if (value < 0) {
      return "-" + format(-value, pad, suffixes);
    }
    if (value < 1000) {
      return padLeft(Long.toString(value), pad ? 4 : 0);
    }

    Map.Entry<Long, String> e = suffixes.floorEntry(value);
    Long divideBy = e.getKey();
    String suffix = e.getValue();

    long truncated = value / (divideBy / 10);
    boolean hasDecimal = truncated < 100 && (truncated % 10 != 0);
    return padLeft(hasDecimal ? (truncated / 10d) + suffix : (truncated / 10) + suffix, pad ? 4 : 0);
  }

  public static String formatPercent(double value) {
    return pf.format(value);
  }

  public static String formatDecimal(double value) {
    return nf.format(value);
  }

  public static String formatInteger(Number value) {
    return intF.format(value);
  }

  public static String formatSeconds(Duration duration) {
    double seconds = duration.toNanos() * 1d / Duration.ofSeconds(1).toNanos();
    return formatDecimal(seconds < 1 ? seconds : Math.round(seconds)) + "s";
  }
}
