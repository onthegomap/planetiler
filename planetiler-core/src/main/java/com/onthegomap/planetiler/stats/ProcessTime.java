package com.onthegomap.planetiler.stats;

import com.onthegomap.planetiler.util.Format;
import java.time.Duration;
import java.util.Optional;

/**
 * A utility for measuring the wall and CPU time that this JVM consumes between snapshots.
 * <p>
 * For example:
 * <pre>{@code
 * var start = ProcessTime.now();
 * // do expensive work...
 * var end - ProcessTime.now();
 * LOGGER.log("Expensive work took " + end.minus(start));
 * }</pre>
 */
public record ProcessTime(Duration wall, Optional<Duration> cpu) {

  /** Takes a snapshot of current wall and CPU time of this JVM. */
  public static ProcessTime now() {
    return new ProcessTime(Duration.ofNanos(System.nanoTime()), ProcessInfo.getProcessCpuTime());
  }

  /** Returns the amount of time elapsed between {@code other} and {@code this}. */
  ProcessTime minus(ProcessTime other) {
    return new ProcessTime(wall.minus(other.wall), cpu.flatMap(thisCpu -> other.cpu.map(thisCpu::minus)));
  }

  @Override
  public String toString() {
    Optional<String> deltaCpu = cpu.map(Format::formatSeconds);
    String avgCpus = cpu.map(cpuTime -> " avg:" + Format.formatDecimal(cpuTime.toNanos() * 1d / wall.toNanos()))
      .orElse("");
    return Format.formatSeconds(wall) + " cpu:" + deltaCpu.orElse("-") + avgCpus;
  }
}
