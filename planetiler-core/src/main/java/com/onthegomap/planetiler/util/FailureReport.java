package com.onthegomap.planetiler.util;

import static com.onthegomap.planetiler.util.LogUtil.UNTIMED_LOGGER;

import com.onthegomap.planetiler.config.ArgumentHelp;
import com.onthegomap.planetiler.reader.FileFormatException;
import com.onthegomap.planetiler.stats.ProcessInfo;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.nio.file.AccessDeniedException;
import java.nio.file.FileSystemException;
import java.nio.file.NoSuchFileException;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import org.slf4j.Logger;

/**
 * Says why planetiler stopped, for the top of a command line entry point.
 * <p>
 * Expected failures (bad options or inputs, running out of disk or memory, errors from the OS reading or writing files)
 * print one line saying what to do, with the stack trace only when asked for. Anything else is a bug, so it prints the
 * stack trace and where to report it.
 */
public class FailureReport {

  private static final Format FORMAT = Format.defaultInstance();

  private FailureReport() {}

  /**
   * Logs why the run stopped with {@code failure}.
   *
   * @param failure what the entry point caught
   * @param verbose true to include the stack trace for expected failures too
   */
  public static void log(Throwable failure, boolean verbose) {
    log(failure, verbose, UNTIMED_LOGGER);
  }

  static void log(Throwable failure, boolean verbose, Logger logger) {
    String expected = expectedFailureMessage(failure);
    if (expected == null) {
      logBug(failure, logger);
    } else if (verbose) {
      logger.error(expected, failure);
    } else {
      logger.error(expected);
    }
  }

  /**
   * Returns the line to print for {@code failure} if it is a problem the user can fix, or {@code null} if it looks like
   * a bug.
   */
  public static String expectedFailureMessage(Throwable failure) {
    boolean fromOtherThread = false;
    for (Throwable e = failure; e != null; e = e.getCause()) {
      if (e instanceof OutOfMemoryError) {
        return "Ran out of memory (current limit is %sB). Try a larger limit like %s.".formatted(
          FORMAT.storage(ProcessInfo.getMaxMemoryBytes()),
          ArgumentHelp.memoryLimitExample(ProcessInfo.getMaxMemoryBytes() * 2));
      } else if (e instanceof InsufficientResourcesException) {
        return e.getMessage();
      } else if (e instanceof IOException io) {
        return describe(io);
      } else if (e instanceof ExecutionException || e instanceof CompletionException) {
        fromOtherThread = true;
      } else if ((e instanceof IllegalArgumentException || e instanceof FileFormatException) && !fromOtherThread) {
        // Code on the main thread throws these for bad options or inputs, but from a worker they are more likely a bug.
        return Objects.requireNonNullElse(e.getMessage(), e + " (run with --verbose for details)");
      }
    }
    return null;
  }

  private static String describe(IOException e) {
    return switch (e) {
      case AccessDeniedException fs -> "Permission denied: " + fs.getFile();
      case NoSuchFileException fs -> "No such file or directory: " + fs.getFile();
      case FileSystemException fs when fs.getReason() == null -> fs.getClass().getSimpleName() + ": " + fs.getFile();
      default -> Objects.requireNonNullElse(e.getMessage(), e.toString());
    };
  }

  private static void logBug(Throwable failure, Logger logger) {
    Duration elapsed = Duration.ofMillis(ManagementFactory.getRuntimeMXBean().getUptime());
    String stage = LogUtil.getStage();
    logger.atError().setMessage("Failed after {}{}:")
      .addArgument(() -> FORMAT.duration(elapsed))
      .addArgument(stage == null ? "" : " in " + stage)
      .setCause(failure)
      .log();
    logger.atInfo()
      .setMessage("""
        This may be a bug. Please report it with the logs above to https://github.com/onthegomap/planetiler/issues
        {}""")
      .addArgument(BuildInfo::versionDetails)
      .log();
  }
}
