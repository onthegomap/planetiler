package com.onthegomap.flatmap.monitoring;

import com.onthegomap.flatmap.Format;
import java.util.LinkedHashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Timers {

  private static final Logger LOGGER = LoggerFactory.getLogger(Stats.InMemory.class);
  private final Map<String, Timer> timers = new LinkedHashMap<>();

  public void time(String name, Runnable task) {
    Finishable timer = startTimer(name);
    task.run();
    timer.stop();
  }

  public void printSummary() {
    LOGGER.info("-".repeat(50));
    int pad = 1 + timers.keySet().stream().mapToInt(String::length).max().orElse("# features".length());
    for (var entry : timers.entrySet()) {
      LOGGER.info(Format.padLeft(entry.getKey(), pad) + ": " + entry.getValue());
    }
  }

  public Finishable startTimer(String name) {
    Timer timer = new Timer().start();
    timers.put(name, timer);
    LOGGER.info("[" + name + "] Starting...");
    return () -> LOGGER.info("[" + name + "] Finished in " + timers.get(name).stop());
  }

  public interface Finishable {

    void stop();
  }
}
