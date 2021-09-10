package com.onthegomap.flatmap.util.log4j;

import java.time.Duration;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.lookup.StrLookup;

/**
 * A log4j plugin that substitutes {@code $${uptime:now}} pattern with the elapsed time of the program in {@code
 * H:MM:SS} form.
 * <p>
 * log4j properties file needs to include {@code packages=com.onthegomap.flatmap.util.log4j} to look in this package for
 * plugins.
 */
@Plugin(name = "uptime", category = StrLookup.CATEGORY)
public class ElapsedTimeLookupPlugin implements StrLookup {

  // rough approximation for start time: when log4j first loads this plugin
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
