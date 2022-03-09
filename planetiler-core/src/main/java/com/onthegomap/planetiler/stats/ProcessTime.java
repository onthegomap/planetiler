package com.onthegomap.planetiler.stats;

import com.onthegomap.planetiler.util.Format;
import java.time.Duration;
import java.util.Locale;
import java.util.Optional;

/**
 * A utility for measuring the wall and CPU time that this JVM consumes between snapshots.
 * <p>
 * For example:
 * 
 * <pre>
 * {@code
 * var start = ProcessTime.now();
 * // do expensive work...
 * var end - ProcessTime.now();
 * LOGGER.log("Expensive work took " + end.minus(start));
 * }
 * </pre>
 */
public record ProcessTime(Duration wall, Optional<Duration> cpu, Duration gc) {

  /** Takes a snapshot of current wall and CPU time of this JVM. */
  public static ProcessTime now() {
    return new ProcessTime(Duration.ofNanos(System.nanoTime()), ProcessInfo.getProcessCpuTime(),
      ProcessInfo.getGcTime());
  }

  /** Returns the amount of time elapsed between {@code other} and {@code this}. */
  ProcessTime minus(ProcessTime other) {
    return new ProcessTime(
      wall.minus(other.wall),
      cpu.flatMap(thisCpu -> other.cpu.map(thisCpu::minus)),
      gc.minus(other.gc)
    );
  }

  public String toString(Locale locale) {
    Format format = Format.forLocale(locale);
    Optional<String> deltaCpu = cpu.map(format::duration);
    String avgCpus = cpu.map(cpuTime -> " avg:" + format.decimal(cpuTime.toNanos() * 1d / wall.toNanos()))
      .orElse("");
    String gcString = gc.compareTo(Duration.ofSeconds(1)) > 0 ? (" gc:" + format.duration(gc)) : "";
    return format.duration(wall) + " cpu:" + deltaCpu.orElse("-") + gcString + avgCpus;
  }

  @Override
  public String toString() {
    return toString(Format.DEFAULT_LOCALE);
  }
}
