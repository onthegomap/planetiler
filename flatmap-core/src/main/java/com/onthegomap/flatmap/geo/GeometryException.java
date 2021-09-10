package com.onthegomap.flatmap.geo;

import com.onthegomap.flatmap.stats.Stats;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An error caused by unexpected input geometry that should be handled to avoid halting the entire program for bad data
 * we are sure to encounter in the wild.
 */
public class GeometryException extends Exception {

  private static final Logger LOGGER = LoggerFactory.getLogger(GeometryException.class);

  private final String stat;

  /**
   * Constructs a new exception with a detailed error message caused by {@code cause}.
   *
   * @param stat    string that uniquely defines this error that will be used to count number of occurrences in stats
   * @param message description of the error to log that should be detailed enough that you can find the offending
   *                geometry from it
   * @param cause   the original exception that was thrown
   */
  public GeometryException(String stat, String message, Throwable cause) {
    super(message, cause);
    this.stat = stat;
  }

  /**
   * Constructs a new exception with a detailed error message.
   *
   * @param stat    string that uniquely defines this error that will be used to count number of occurrences in stats
   * @param message description of the error to log that should be detailed enough that you can find the offending
   *                geometry from it
   */
  public GeometryException(String stat, String message) {
    super(message);
    this.stat = stat;
  }

  /** Returns the unique code for this error condition to use for counting the number of occurrences in stats. */
  public String stat() {
    return stat;
  }

  /** Prints the error and also increments a stat counter for this error and logs it. */
  public void log(Stats stats, String statPrefix, String logPrefix) {
    stats.dataError(statPrefix + "_" + stat());
    log(logPrefix);
  }

  /** Prints the error but does not increment any stats. */
  public void log(String logContext) {
    logMessage(logContext + ": " + getMessage());
  }

  void logMessage(String log) {
    LOGGER.warn(log);
  }

  /**
   * An error that we expect to encounter often so should only be logged at {@code TRACE} level.
   */
  public static class Verbose extends GeometryException {

    /**
     * Constructs a new verbose exception with a detailed error message caused by {@code cause}.
     *
     * @param stat    string that uniquely defines this error that will be used to count number of occurrences in stats
     * @param message description of the error to log that should be detailed enough that you can find the offending
     *                geometry from it
     * @param cause   the original exception that was thrown
     */
    public Verbose(String stat, String message, Throwable cause) {
      super(stat, message, cause);
    }

    /**
     * Constructs a new verbose exception with a detailed error message.
     *
     * @param stat    string that uniquely defines this error that will be used to count number of occurrences in stats
     * @param message description of the error to log that should be detailed enough that you can find the offending
     *                geometry from it
     */
    public Verbose(String stat, String message) {
      super(stat, message);
    }

    @Override
    void logMessage(String log) {
      LOGGER.trace(log);
    }
  }
}
