package com.onthegomap.flatmap.worker;

import com.onthegomap.flatmap.ProgressLoggers;
import com.onthegomap.flatmap.stats.Stats;

public class Worker {

  public Worker(String name, Stats stats, int threads, Runnable task) {

  }

  public String getPrefix() {
    return null;
  }

  public void awaitAndLog(ProgressLoggers loggers, long logIntervalSeconds) {
  }
}
