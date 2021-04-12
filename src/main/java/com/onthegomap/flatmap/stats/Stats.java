package com.onthegomap.flatmap.stats;

import com.graphhopper.util.StopWatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public interface Stats {

  void time(String name, Runnable task);

  void printSummary();

  void startTimer(String name);

  void stopTimer(String name);

  void encodedTile(int zoom, int length);

  class InMemory implements Stats {

    private static final Logger LOGGER = LoggerFactory.getLogger(InMemory.class);

    @Override
    public void time(String name, Runnable task) {
      StopWatch timer = new StopWatch().start();
      LOGGER.info("[" + name + "] Starting...");
      task.run();
      LOGGER.info("[" + name + "] Finished in " + timer.stop());
    }

    @Override
    public void printSummary() {

    }

    @Override
    public void startTimer(String name) {

    }

    @Override
    public void stopTimer(String name) {

    }

    @Override
    public void encodedTile(int zoom, int length) {

    }
  }
}
