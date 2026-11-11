package com.onthegomap.planetiler.util.log4j;

import java.time.Duration;
import java.util.Map;
import org.apache.logging.log4j.core.util.ContextDataProvider;

/**
 * Adds an {@code uptime} entry with the elapsed time of the program in {@code H:MM:SS} form to the context data of
 * every log event, so log patterns can print it with {@code %X{uptime}}.
 */
public class UptimeContextDataProvider implements ContextDataProvider {

  // rough approximation for start time: when log4j first loads this class
  private static final long START_NANOS = System.nanoTime();

  @Override
  public Map<String, String> supplyContextData() {
    Duration elapsed = Duration.ofNanos(System.nanoTime() - START_NANOS);
    return Map.of("uptime", "%d:%02d:%02d".formatted(
      elapsed.toHours(),
      elapsed.toMinutesPart(),
      elapsed.toSecondsPart()
    ));
  }
}
