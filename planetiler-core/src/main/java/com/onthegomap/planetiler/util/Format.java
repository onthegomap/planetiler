package com.onthegomap.planetiler.util;

import java.text.NumberFormat;
import java.time.Duration;
import java.util.Locale;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.commons.text.StringEscapeUtils;
import org.locationtech.jts.geom.Coordinate;

/**
 * Utilities for formatting values as strings.
 */
public class Format {

  public static final Locale DEFAULT_LOCALE = Locale.getDefault(Locale.Category.FORMAT);
  public static final ConcurrentMap<Locale, Format> instances = new ConcurrentHashMap<>();
  private final NumberFormat pf;
  private final NumberFormat nf;
  private final NumberFormat intF;

  private Format(Locale locale) {
    pf = NumberFormat.getPercentInstance(locale);
    pf.setMaximumFractionDigits(0);
    nf = NumberFormat.getNumberInstance(locale);
    nf.setMaximumFractionDigits(1);
    intF = NumberFormat.getNumberInstance(locale);
    intF.setMaximumFractionDigits(0);
  }

  public static Format forLocale(Locale locale) {
    Format format = instances.get(locale);
    if (format == null) {
      format = instances.computeIfAbsent(locale, Format::new);
    }
    return format;
  }

  public static Format defaultInstance() {
    return forLocale(DEFAULT_LOCALE);
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
  public String storage(Number num, boolean pad) {
    return format(num, pad, STORAGE_SUFFIXES);
  }

  /** Returns a number formatted like "123" "1.2k" "2.5B", etc. */
  public String numeric(Number num, boolean pad) {
    return format(num, pad, NUMERIC_SUFFIXES);
  }

  private String format(Number num, boolean pad, NavigableMap<Long, String> suffixes) {
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
    return padLeft(hasDecimal ? decimal(truncated / 10d) + suffix : (truncated / 10) + suffix, pad ? 4 : 0);
  }

  /** Returns 0.0-1.0 as a "0%" - "100%" with no decimal points. */
  public String percent(double value) {
    return pf.format(value);
  }

  /** Returns a number formatted with 1 decimal point. */
  public String decimal(double value) {
    return nf.format(value);
  }

  /** Returns a number formatted with 0 decimal points. */
  public String integer(Number value) {
    return intF.format(value);
  }

  /** Returns a duration formatted as fractional seconds with 1 decimal point. */
  public String seconds(Duration duration) {
    double seconds = duration.toNanos() * 1d / Duration.ofSeconds(1).toNanos();
    return decimal(seconds < 1 ? seconds : Math.round(seconds)) + "s";
  }

  /** Returns a duration formatted like "1h2m" or "2m3s". */
  public String duration(Duration duration) {
    Duration simplified;
    double seconds = duration.toNanos() * 1d / Duration.ofSeconds(1).toNanos();
    if (seconds < 1) {
      return decimal(seconds) + "s";
    } else {
      simplified = Duration.ofSeconds(Math.round(seconds));
    }
    return simplified.toString().replace("PT", "").toLowerCase(Locale.ROOT);
  }

  /** Returns Java code that can re-create {@code string}: {@code null} if null, or {@code "contents"} if not empty. */
  public static String quote(String string) {
    if (string == null) {
      return "null";
    }
    return '"' + StringEscapeUtils.escapeJava(string) + '"';
  }

  /** Returns an openstreetmap.org map link for a lat/lon */
  public static String osmDebugUrl(int zoom, Coordinate coord) {
    return "https://www.openstreetmap.org/#map=%d/%.5f/%.5f".formatted(
      zoom,
      coord.y,
      coord.x
    );
  }
}
