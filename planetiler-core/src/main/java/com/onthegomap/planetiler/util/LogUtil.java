package com.onthegomap.planetiler.util;

import java.util.regex.Pattern;
import org.slf4j.MDC;

/**
 * Wrapper for SLF4j {@link MDC} log utility to prepend {@code [stage]} to log output.
 */
public class LogUtil {

  private LogUtil() {}

  private static final String STAGE_KEY = "stage";

  /** Prepends {@code [stage]} to all subsequent logs from this thread. */
  public static void setStage(String stage) {
    MDC.put(STAGE_KEY, stage);
  }

  /** Removes {@code [stage]} from subsequent logs from this thread. */
  public static void clearStage() {
    MDC.remove(STAGE_KEY);
  }

  /** Returns the current {@code [stage]} value prepended to log for this thread. */
  public static String getStage() {
    return MDC.get(STAGE_KEY);
  }

  /** Prepends {@code [parent:child]} to all subsequent logs from this thread. */
  public static void setStage(String parent, String child) {
    if (parent == null) {
      setStage(child);
    } else {
      setStage(parent + ":" + child.replaceFirst("^" + Pattern.quote(parent) + "_?", ""));
    }
  }
}
