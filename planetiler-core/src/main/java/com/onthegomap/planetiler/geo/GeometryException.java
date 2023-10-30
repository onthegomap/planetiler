package com.onthegomap.planetiler.geo;

import com.onthegomap.planetiler.stats.Stats;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An error caused by unexpected input geometry that should be handled to avoid halting the entire program for bad data
 * we are sure to encounter in the wild.
 */
public class GeometryException extends Exception {

  private static final Logger LOGGER = LoggerFactory.getLogger(GeometryException.class);

  private final String stat;
  private final boolean nonFatal;
  private Supplier<String> detailsSupplier;

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
    this.nonFatal = false;
  }

  /**
   * Constructs a new exception with a detailed error message for. Use
   * {@link #GeometryException(String, String, boolean)} for non-fatal exceptions.
   */
  public GeometryException(String stat, String message) {
    this(stat, message, false);
  }

  /**
   * Constructs a new exception with a detailed error message.
   *
   * @param stat     string that uniquely defines this error that will be used to count number of occurrences in stats
   * @param message  description of the error to log that should be detailed enough that you can find the offending
   *                 geometry from it
   * @param nonFatal When true, won't cause an assertion error when thrown
   */
  public GeometryException(String stat, String message, boolean nonFatal) {
    super(message);
    this.stat = stat;
    this.nonFatal = nonFatal;
  }

  public GeometryException addDetails(Supplier<String> detailsSupplier) {
    this.detailsSupplier = detailsSupplier;
    return this;
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
    assert nonFatal : log; // make unit tests fail if fatal
  }


  /** Logs the error but if {@code logDetails} is true, then also prints detailed debugging info. */
  public void log(Stats stats, String statPrefix, String logPrefix, boolean logDetails) {
    if (logDetails && detailsSupplier != null) {
      stats.dataError(statPrefix + "_" + stat());
      String log = logPrefix + ": " + getMessage() + "\n" + detailsSupplier.get();
      LOGGER.warn(log, this);
      assert nonFatal : log; // make unit tests fail if fatal
    } else {
      log(stats, statPrefix, logPrefix);
    }
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
