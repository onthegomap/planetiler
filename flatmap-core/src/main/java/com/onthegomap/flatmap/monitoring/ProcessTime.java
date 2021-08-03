package com.onthegomap.flatmap.monitoring;

import com.onthegomap.flatmap.Format;
import java.time.Duration;
import java.util.Optional;

public record ProcessTime(Duration wall, Optional<Duration> cpu) {

  public static ProcessTime now() {
    return new ProcessTime(Duration.ofNanos(System.nanoTime()), ProcessInfo.getProcessCpuTime());
  }

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
