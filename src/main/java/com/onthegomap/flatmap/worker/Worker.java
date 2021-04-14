package com.onthegomap.flatmap.worker;

import com.onthegomap.flatmap.ProgressLoggers;
import com.onthegomap.flatmap.stats.Stats;
import java.time.Duration;

public class Worker {

  public Worker(String prefix, String name, Stats stats, int threads, RunnableThatThrows task) {

  }

  public String getPrefix() {
    return null;
  }

  public void awaitAndLog(ProgressLoggers loggers, Duration longInterval) {
  }

  public interface RunnableThatThrows {

    void run() throws Exception;
  }
}
