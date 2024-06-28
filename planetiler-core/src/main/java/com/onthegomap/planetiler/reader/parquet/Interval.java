package com.onthegomap.planetiler.reader.parquet;

import java.time.Duration;
import java.time.Period;
import java.time.temporal.Temporal;
import java.time.temporal.TemporalAmount;
import java.time.temporal.TemporalUnit;
import java.util.List;
import java.util.stream.Stream;

/**
 * Represents a <a href="https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#interval">parquet
 * interval</a> datatype which has a month, day, and millisecond part.
 * <p>
 * Built-in java {@link TemporalAmount} implementations can only store a period or duration amount, but not both.
 */
public record Interval(Period period, Duration duration) implements TemporalAmount {

  public static Interval of(int months, long days, long millis) {
    return new Interval(Period.ofMonths(months).plusDays(days), Duration.ofMillis(millis));
  }

  @Override
  public long get(TemporalUnit unit) {
    return period.get(unit) + duration.get(unit);
  }

  @Override
  public List<TemporalUnit> getUnits() {
    return Stream.concat(period.getUnits().stream(), duration.getUnits().stream()).toList();
  }

  @Override
  public Temporal addTo(Temporal temporal) {
    return temporal.plus(period).plus(duration);
  }

  @Override
  public Temporal subtractFrom(Temporal temporal) {
    return temporal.minus(period).minus(duration);
  }
}
