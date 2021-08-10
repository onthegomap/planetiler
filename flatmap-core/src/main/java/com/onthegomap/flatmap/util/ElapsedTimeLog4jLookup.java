package com.onthegomap.flatmap.util;

import java.time.Duration;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.lookup.StrLookup;

/**
 * A log4j plugin that substitutes {@code $${uptime:now}} pattern with the elapsed time of the program in H:MM:SS form.
 */
@Plugin(name = "uptime", category = StrLookup.CATEGORY)
public class ElapsedTimeLog4jLookup implements StrLookup {

  private static final long startTime = System.nanoTime();

  @Override
  public String lookup(String key) {
    Duration duration = Duration.ofNanos(System.nanoTime() - startTime);

    return "%d:%02d:%02d".formatted(
      duration.toHours(),
      duration.toMinutesPart(),
      duration.toSecondsPart()
    );
  }

  @Override
  public String lookup(LogEvent event, String key) {
    return lookup(key);
  }
}
