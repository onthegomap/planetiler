package com.onthegomap.planetiler.util;

import java.util.regex.Pattern;
import org.apache.logging.log4j.core.config.Configurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.slf4j.event.Level;

/**
 * Wrapper for SLF4j {@link MDC} log utility to prepend {@code [stage]} to log output, and to change how much gets
 * logged.
 */
public class LogUtil {
  /** A logger whose lines don't start with the elapsed time, for printing after run finishes. */
  public static final Logger UNTIMED_LOGGER = LoggerFactory.getLogger("untimed");

  private static final String STAGE_KEY = "stage";

  private LogUtil() {}

  /** Prepends {@code [stage]} to all subsequent logs from this thread. */
  public static void setStage(String stage) {
    MDC.put(STAGE_KEY, "[%s] ".formatted(stage));
  }

  /** Removes {@code [stage]} from subsequent logs from this thread. */
  public static void clearStage() {
    MDC.remove(STAGE_KEY);
  }

  /** Returns the current {@code [stage]} value prepended to log for this thread. */
  public static String getStage() {
    // strip out the "[stage] " wrapper
    return MDC.get(STAGE_KEY) instanceof String s ? s.substring(1, s.length() - 2) : null;
  }

  /** Prepends {@code [parent:child]} to all subsequent logs from this thread. */
  public static void setStage(String parent, String child) {
    if (parent == null) {
      setStage(child);
    } else {
      setStage(parent + ":" + child.replaceFirst("^" + Pattern.quote(parent) + "_?", ""));
    }
  }

  /**
   * Runs {@code task} with {@code [stage]} prepended to the logs from this thread, then goes back to the stage before.
   */
  public static void runInStage(String stage, Runnable task) {
    String previousStage = getStage();
    setStage(stage);
    try {
      task.run();
    } finally {
      if (previousStage == null) {
        clearStage();
      } else {
        setStage(previousStage);
      }
    }
  }

  /**
   * Logs only messages at {@code level} or above from now on, from every logger that doesn't set its own level: e.g.
   * {@link Level#DEBUG} for {@code --verbose} or {@link Level#WARN} for {@code --quiet}.
   */
  public static void setLevel(Level level) {
    Configurator.setRootLevel(org.apache.logging.log4j.Level.valueOf(level.name()));
  }
}
