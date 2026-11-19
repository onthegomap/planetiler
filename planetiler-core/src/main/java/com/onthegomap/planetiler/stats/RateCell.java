package com.onthegomap.planetiler.stats;

import static com.onthegomap.planetiler.util.AnsiColors.green;
import static com.onthegomap.planetiler.util.Format.padLeft;

import com.onthegomap.planetiler.util.Format;
import java.util.function.Function;
import java.util.function.LongSupplier;
import java.util.function.LongToDoubleFunction;

/**
 * A count shown as {@code [ value pct% rate/s ]} in the progress logs, with the rate since the last time it was shown.
 * <p>
 * Only the thread that logs progress may call {@link #render}.
 */
final class RateCell {
  private final Format format;
  private final LongSupplier value;
  // null for counts that have no total, which leaves the percent out
  private final LongToDoubleFunction fractionDone;
  private final Function<Number, String> formatPadded;
  private final boolean color;
  private long lastValue;
  private double lastFraction = Double.NaN;
  private long lastNanos;

  /**
   * Shows {@code value} formatted with {@code formatPadded}, and {@code fractionDone} of that value as a percent unless
   * it is null, or as {@code -} while it returns NaN. If {@code color} is true, it is green while it moves.
   */
  RateCell(Format format, LongSupplier value, LongToDoubleFunction fractionDone, Function<Number, String> formatPadded,
    boolean color) {
    this(format, value, fractionDone, formatPadded, color, value.getAsLong(), System.nanoTime());
  }

  /** Like the other constructor, for a count that was {@code startValue} at {@code startNanos}. */
  RateCell(Format format, LongSupplier value, LongToDoubleFunction fractionDone, Function<Number, String> formatPadded,
    boolean color, long startValue, long startNanos) {
    this.format = format;
    this.value = value;
    this.fractionDone = fractionDone;
    this.formatPadded = formatPadded;
    this.color = color;
    this.lastValue = startValue;
    this.lastNanos = startNanos;
  }

  String render() {
    return render(false);
  }

  /** Renders the cell, green if it moved since the last time or {@code busy} is true. */
  String render(boolean busy) {
    long now = System.nanoTime();
    long valueNow = value.getAsLong();
    double fraction = fractionDone == null ? Double.NaN : fractionDone.applyAsDouble(valueNow);
    long valueDiff = valueNow - lastValue;
    // a counter that went back to 0 counts everything since then
    if (valueDiff < 0) {
      valueDiff = valueNow;
    }
    double rate = valueDiff * 1e9 / Math.max(1, now - lastNanos);
    // the percent can move without the count, like a source that reads a few huge features from many blocks
    boolean moved = valueDiff > 0 || fraction > lastFraction;
    lastValue = valueNow;
    lastFraction = fraction;
    lastNanos = now;
    String result = render(valueNow, fraction, rate);
    return color && (moved || busy) ? green(result) : result;
  }

  private String render(Number value, double fraction, Number rate) {
    return "[ " + formatPadded.apply(value) + percent(fraction) + " " + formatPadded.apply(rate) + "/s ]";
  }

  /** Returns the number of characters {@link #render} returns, not counting colors. */
  int width() {
    return render(0, 0, 0).length();
  }

  private String percent(double fraction) {
    return fractionDone == null ? "" : " " + padLeft(Double.isNaN(fraction) ? "-" : format.percent(fraction), 4);
  }
}
