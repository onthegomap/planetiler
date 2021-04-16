package com.onthegomap.flatmap.monitoring;

import com.graphhopper.util.StopWatch;
import java.util.Map;
import java.util.TreeMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public interface Stats {

  void time(String name, Runnable task);

  void printSummary();

  void startTimer(String name);

  void stopTimer(String name);

  void encodedTile(int zoom, int length);

  void gauge(String name, int value);

  class InMemory implements Stats {

    private static final Logger LOGGER = LoggerFactory.getLogger(InMemory.class);
    private Map<String, StopWatch> timers = new TreeMap<>();

    @Override
    public void time(String name, Runnable task) {
      startTimer(name);
      task.run();
      stopTimer(name);
    }

    @Override
    public void printSummary() {

    }

    @Override
    public void startTimer(String name) {
      timers.put(name, new StopWatch().start());
      LOGGER.info("[" + name + "] Starting...");
    }

    @Override
    public void stopTimer(String name) {
      LOGGER.info("[" + name + "] Finished in " + timers.get(name).stop());
    }

    @Override
    public void encodedTile(int zoom, int length) {

    }

    @Override
    public void gauge(String name, int value) {

    }
  }
}
