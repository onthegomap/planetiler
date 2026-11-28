package com.onthegomap.planetiler.util;

import java.util.concurrent.atomic.AtomicInteger;
import net.jcip.annotations.ThreadSafe;
import org.slf4j.Logger;

/**
 * Logs an error that may happen for many items, like a profile that throws for every road, with the stack trace for
 * only the first few so the log stays readable.
 */
@ThreadSafe
public class RepeatedErrorLogger {

  private final Logger logger;
  private final int maxStackTraces;
  private final AtomicInteger logged = new AtomicInteger();

  /** Creates a logger that logs to {@code logger}, with stack traces for the first {@code maxStackTraces} errors. */
  public RepeatedErrorLogger(Logger logger, int maxStackTraces) {
    this.logger = logger;
    this.maxStackTraces = maxStackTraces;
  }

  /** Logs {@code message} at error level, followed by the stack trace of {@code error} or one line describing it. */
  public void log(String message, Throwable error) {
    if (logger.isErrorEnabled()) {
      int count = logged.incrementAndGet();
      if (count <= maxStackTraces) {
        logger.error(message, error);
      } else if (count == maxStackTraces + 1) {
        logger.error("{}: {} (leaving out stack traces after the first {})", message, error, maxStackTraces);
      } else {
        logger.error("{}: {}", message, error.toString());
      }
    }
  }
}
