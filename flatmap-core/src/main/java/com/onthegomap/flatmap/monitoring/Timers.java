package com.onthegomap.flatmap.monitoring;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Timers {

  private static final Logger LOGGER = LoggerFactory.getLogger(Stats.InMemory.class);
  private final Map<String, Timer> timers = Collections.synchronizedMap(new LinkedHashMap<>());

  public void printSummary() {
    LOGGER.info("-".repeat(50));
    synchronized (timers) {
      for (var entry : timers.entrySet()) {
        LOGGER.info("\t" + entry.getKey() + "\t" + entry.getValue().elapsed());
      }
    }
  }

  public Finishable startTimer(String name) {
    Timer timer = new Timer().start();
    timers.put(name, timer);
    LOGGER.info("[" + name + "] Starting...");
    return () -> LOGGER.info("[" + name + "] Finished in " + timers.get(name).stop() + "\n");
  }

  public Map<String, Timer> all() {
    synchronized (timers) {
      return new LinkedHashMap<>(timers);
    }
  }

  public interface Finishable {

    void stop();
  }
}
