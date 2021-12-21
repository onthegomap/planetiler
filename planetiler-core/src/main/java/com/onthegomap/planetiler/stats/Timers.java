package com.onthegomap.planetiler.stats;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import javax.annotation.concurrent.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A registry of tasks that are being timed.
 */
@ThreadSafe
public class Timers {

  private static final Logger LOGGER = LoggerFactory.getLogger(Stats.InMemory.class);
  private final Map<String, Timer> timers = Collections.synchronizedMap(new LinkedHashMap<>());

  public void printSummary() {
    for (var entry : all().entrySet()) {
      LOGGER.info("\t" + entry.getKey() + "\t" + entry.getValue().elapsed());
    }
  }

  public Finishable startTimer(String name) {
    Timer timer = Timer.start();
    timers.put(name, timer);
    LOGGER.info("Starting...");
    return () -> LOGGER.info("Finished in " + timers.get(name).stop() + System.lineSeparator());
  }

  /** Returns a snapshot of all timers currently running. Will not reflect timers that start after it's called. */
  public Map<String, Timer> all() {
    synchronized (timers) {
      return new LinkedHashMap<>(timers);
    }
  }

  /** A handle that callers can use to indicate a task has finished. */
  public interface Finishable {

    void stop();
  }
}
