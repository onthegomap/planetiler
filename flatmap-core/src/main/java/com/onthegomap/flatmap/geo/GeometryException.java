package com.onthegomap.flatmap.geo;

import com.onthegomap.flatmap.monitoring.Stats;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GeometryException extends Exception {

  private static final Logger LOGGER = LoggerFactory.getLogger(GeometryException.class);

  private final String stat;

  public GeometryException(String stat, String message, Throwable cause) {
    super(message, cause);
    this.stat = stat;
  }

  public GeometryException(String stat, String message) {
    super(message);
    this.stat = stat;
  }

  public String stat() {
    return stat;
  }

  public void log(Stats stats, String statContext, String logContext) {
    stats.dataError(statContext + "_" + stat());
    log(logContext);
  }

  public void log(String logContext) {
    logMessage(logContext + ": " + getMessage());
  }

  void logMessage(String log) {
    LOGGER.warn(log);
  }

  public static class Verbose extends GeometryException {

    public Verbose(String stat, String message, Throwable cause) {
      super(stat, message, cause);
    }

    public Verbose(String stat, String message) {
      super(stat, message);
    }

    @Override
    void logMessage(String log) {
      LOGGER.trace(log);
    }
  }
}
