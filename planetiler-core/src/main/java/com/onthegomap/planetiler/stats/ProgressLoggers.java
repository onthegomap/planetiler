package com.onthegomap.planetiler.stats;

import static com.onthegomap.planetiler.util.Format.*;

import com.graphhopper.util.Helper;
import com.onthegomap.planetiler.util.DiskBacked;
import com.onthegomap.planetiler.util.Format;
import com.onthegomap.planetiler.util.MemoryEstimator;
import com.onthegomap.planetiler.worker.WorkQueue;
import com.onthegomap.planetiler.worker.Worker;
import com.onthegomap.planetiler.worker.WorkerPipeline;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.DoubleFunction;
import java.util.function.Function;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Logs the progress of a long-running task (percent complete, queue sizes, CPU and memory usage, etc.)
 */
@SuppressWarnings({"UnusedReturnValue", "unused"})
public class ProgressLoggers {

  private static final String COLOR_RESET = "\u001B[0m";
  private static final String FG_RED = "\u001B[31m";
  private static final String FG_GREEN = "\u001B[32m";
  private static final String FG_YELLOW = "\u001B[33m";
  private static final String FG_BLUE = "\u001B[34m";
  private static final Logger LOGGER = LoggerFactory.getLogger(ProgressLoggers.class);
  private final List<Object> loggers = new ArrayList<>();

  private static String fg(String fg, String string) {
    return fg + string + COLOR_RESET;
  }

  private static String red(String string) {
    return fg(FG_RED, string);
  }

  private static String green(String string) {
    return fg(FG_GREEN, string);
  }

  private static String yellow(String string) {
    return fg(FG_YELLOW, string);
  }

  private static String blue(String string) {
    return fg(FG_BLUE, string);
  }

  private ProgressLoggers() {
  }

  public static ProgressLoggers create() {
    return new ProgressLoggers();
  }

  /** Adds "name: [ numCompleted rate/s ]" to the logger. */
  public ProgressLoggers addRateCounter(String name, LongSupplier getValue) {
    return addRateCounter(name, getValue, false);
  }

  /** Adds "name: [ numCompleted rate/s ]" to the logger. */
  public ProgressLoggers addRateCounter(String name, AtomicLong getValue) {
    return addRateCounter(name, getValue, false);
  }

  /**
   * Adds "name: [ numCompleted rate/s ]" to the logger, colored green if {@code color=true} and rate > 0.
   */
  public ProgressLoggers addRateCounter(String name, LongSupplier getValue, boolean color) {
    AtomicLong last = new AtomicLong(getValue.getAsLong());
    AtomicLong lastTime = new AtomicLong(System.nanoTime());
    loggers.add(new ProgressLogger(name, () -> {
      long now = System.nanoTime();
      long valueNow = getValue.getAsLong();
      double timeDiff = (now - lastTime.get()) * 1d / (1d * TimeUnit.SECONDS.toNanos(1));
      double valueDiff = valueNow - last.get();
      if (valueDiff < 0) {
        valueDiff = valueNow;
      }
      last.set(valueNow);
      lastTime.set(now);
      String result = "[ " + formatNumeric(valueNow, true) + " " + formatNumeric(valueDiff / timeDiff, true) + "/s ]";
      return color && valueDiff > 0 ? green(result) : result;
    }));
    return this;
  }

  /**
   * Adds "name: [ numCompleted rate/s ]" to the logger, colored green if {@code color=true} and rate > 0.
   */
  public ProgressLoggers addRateCounter(String name, AtomicLong value, boolean color) {
    return addRateCounter(name, value::get, color);
  }

  /**
   * Adds "name: [ numCompleted pctComplete% rate/s ]" to the logger where {@code total} is the total number of items to
   * process.
   */
  public ProgressLoggers addRatePercentCounter(String name, long total, AtomicLong value) {
    return addRatePercentCounter(name, total, value::get);
  }

  /**
   * Adds "name: [ numCompleted pctComplete% rate/s ]" to the logger where {@code total} is the total number of items to
   * process.
   */
  public ProgressLoggers addRatePercentCounter(String name, long total, LongSupplier getValue) {
    return addRatePercentCounter(name, total, getValue, n -> Format.formatNumeric(n, true));
  }

  /**
   * Adds "name: [ numCompleted pctComplete% rate/s ]" to the logger where {@code total} is the total number of bytes to
   * process.
   */
  public ProgressLoggers addStorageRatePercentCounter(String name, long total, LongSupplier getValue) {
    return addRatePercentCounter(name, total, getValue, n -> Format.formatStorage(n, true));
  }

  /**
   * Adds "name: [ numCompleted pctComplete% rate/s ]" to the logger where {@code total} is the total number of items to
   * process.
   */
  public ProgressLoggers addRatePercentCounter(String name, long total, LongSupplier getValue,
    Function<Number, String> format) {
    // if there's no total, we can't show progress so fall back to rate logger instead
    if (total == 0) {
      return addRateCounter(name, getValue, true);
    }
    AtomicLong last = new AtomicLong(getValue.getAsLong());
    AtomicLong lastTime = new AtomicLong(System.nanoTime());
    loggers.add(new ProgressLogger(name, () -> {
      long now = System.nanoTime();
      long valueNow = getValue.getAsLong();
      double timeDiff = (now - lastTime.get()) * 1d / (1d * TimeUnit.SECONDS.toNanos(1));
      double valueDiff = valueNow - last.get();
      if (valueDiff < 0) {
        valueDiff = valueNow;
      }
      last.set(valueNow);
      lastTime.set(now);
      String result =
        "[ " + format.apply(valueNow) + " " + padLeft(formatPercent(1f * valueNow / total), 4)
          + " " + format.apply(valueDiff / timeDiff) + "/s ]";
      return valueDiff > 0 ? green(result) : result;
    }));
    return this;
  }

  /**
   * Adds "name: [ numComplete / total pctComplete% ]" to the logger where {@code total} is the total number of items to
   * process.
   */
  public ProgressLoggers addPercentCounter(String name, long total, AtomicLong getValue) {
    loggers.add(new ProgressLogger(name, () -> {
      long valueNow = getValue.get();
      return "[ " + padLeft("" + valueNow, 3) + " / " + padLeft("" + total, 3) + " " + padLeft(
        formatPercent(1f * valueNow / total), 4) + " ]";
    }));
    return this;
  }

  /** Adds the current number of items in a queue and the queue's size to the output. */
  public ProgressLoggers addQueueStats(WorkQueue<?> queue) {
    loggers.add(new WorkerPipelineLogger(() ->
      " -> " + padLeft("(" +
        formatNumeric(queue.getPending(), false)
        + "/" +
        formatNumeric(queue.getCapacity(), false)
        + ")", 9)
    ));
    return this;
  }

  public ProgressLoggers add(String obj) {
    loggers.add(obj);
    return this;
  }

  public ProgressLoggers add(Supplier<String> obj) {
    loggers.add(new Object() {
      @Override
      public String toString() {
        return obj.get();
      }
    });
    return this;
  }

  public ProgressLoggers addFileSize(Path file) {
    return add(() -> {
      String bytes;
      try {
        bytes = formatStorage(Files.size(file), false);
      } catch (IOException e) {
        bytes = "-";
      }
      return " " + padRight(bytes, 5);
    });
  }

  public ProgressLoggers addFileSize(DiskBacked longSupplier) {
    return add(() -> " " + padRight(formatStorage(longSupplier.diskUsageBytes(), false), 5));
  }

  /** Adds the total of disk and memory usage of {@code thing}. */
  public <T extends DiskBacked & MemoryEstimator.HasEstimate> ProgressLoggers addFileSizeAndRam(T thing) {
    return add(() -> {
      long bytes = thing.diskUsageBytes() + thing.estimateMemoryUsageBytes();
      return " " + padRight(formatStorage(bytes, false), 5);
    });
  }

  /** Adds the current size of a file on disk. */
  public ProgressLoggers addFileSize(String name, DiskBacked file) {
    loggers.add(new ProgressLogger(name, () -> formatStorage(file.diskUsageBytes(), true)));
    return this;
  }

  /**
   * Adds the average number of CPUs and % time in GC since last log along with memory usage, total memory, and memory
   * used after last GC to the output.
   */
  public ProgressLoggers addProcessStats() {
    addOptionalDeltaLogger("cpus", ProcessInfo::getProcessCpuTime, num -> blue(Format.formatDecimal(num)));
    addDeltaLogger("gc", ProcessInfo::getGcTime, num -> {
      String formatted = Format.formatPercent(num);
      return num > 0.6 ? red(formatted) : num > 0.3 ? yellow(formatted) : formatted;
    });
    loggers.add(new ProgressLogger("mem",
      () -> formatStorage(Helper.getUsedMB() * Helper.MB, false) + "/" +
        formatStorage(Helper.getTotalMB() * Helper.MB, false) +
        ProcessInfo.getMemoryUsageAfterLastGC().stream()
          .mapToObj(value -> " postGC: " + blue(formatStorage(value, false)))
          .findFirst()
          .orElse("")
    ));
    return this;
  }

  private void addOptionalDeltaLogger(String name, Supplier<Optional<Duration>> supplier,
    DoubleFunction<String> format) {
    addDeltaLogger(name, () -> supplier.get().orElse(Duration.ZERO), format);
  }

  // adds a logger that keeps track of the value each time it is invoked and logs the change
  private void addDeltaLogger(String name, Supplier<Duration> supplier, DoubleFunction<String> format) {
    AtomicLong lastValue = new AtomicLong(supplier.get().toNanos());
    AtomicLong lastTime = new AtomicLong(System.nanoTime());
    loggers.add(new ProgressLogger(name, () -> {
      long currentValue = supplier.get().toNanos();
      if (currentValue < 0) {
        return "-";
      }
      long currentTime = System.nanoTime();
      double rate = 1d * (currentValue - lastValue.get()) / (currentTime - lastTime.get());
      lastTime.set(currentTime);
      lastValue.set(currentValue);
      return padLeft(format.apply(rate), 3);
    }));
  }

  /** Adds the CPU utilization of every thread starting with {@code prefix} since the last log to output. */
  public ProgressLoggers addThreadPoolStats(String name, String prefix) {
    boolean first = loggers.isEmpty() || !(loggers.get(loggers.size() - 1) instanceof WorkerPipelineLogger);
    try {
      Map<Long, ProcessInfo.ThreadState> lastThreads = ProcessInfo.getThreadStats();
      AtomicLong lastTime = new AtomicLong(System.nanoTime());
      loggers.add(new WorkerPipelineLogger(() -> {
        var oldAndNewThreads = new TreeMap<>(lastThreads);
        var newThreads = ProcessInfo.getThreadStats();
        oldAndNewThreads.putAll(newThreads);

        long currentTime = System.nanoTime();
        double timeDiff = 1d * (currentTime - lastTime.get());
        String percents = oldAndNewThreads.values().stream()
          .filter(thread -> thread.name().startsWith(prefix))
          .map(thread -> {
            if (!newThreads.containsKey(thread.id())) {
              return " -%";
            }
            long last = lastThreads.getOrDefault(thread.id(), ProcessInfo.ThreadState.DEFAULT).cpuTime().toNanos();
            return padLeft(formatPercent(1d * (thread.cpuTime().toNanos() - last) / timeDiff), 3);
          }).collect(Collectors.joining(" ", "(", ")"));

        lastTime.set(currentTime);
        lastThreads.putAll(newThreads);
        return (first ? " " : " -> ") + name + percents;
      }));
    } catch (Throwable ignored) {
      // can't get CPU stats per-thread
    }
    return this;
  }

  /** Adds the CPU utilization since last log of every thread in a {@link Worker} pool to output. */
  public ProgressLoggers addThreadPoolStats(String name, Worker worker) {
    return addThreadPoolStats(name, worker.getPrefix());
  }

  public void log() {
    LOGGER.info(getLog());
  }

  public String getLog() {
    return loggers.stream()
      .map(Object::toString)
      .collect(Collectors.joining(""))
      .replaceAll(System.lineSeparator() + "\\s*", System.lineSeparator() + "    ");
  }

  /** Adds the current estimated size of an in-memory object to the output. */
  public ProgressLoggers addInMemoryObject(String name, MemoryEstimator.HasEstimate object) {
    loggers.add(new ProgressLogger(name, () -> formatStorage(object.estimateMemoryUsageBytes(), true)));
    return this;
  }

  /** Adds the alternating worker thread pool / queue / worker thread pool stats for the pipeline to the output. */
  public ProgressLoggers addPipelineStats(WorkerPipeline<?> pipeline) {
    if (pipeline != null) {
      addPipelineStats(pipeline.previous());
      if (pipeline.inputQueue() != null) {
        addQueueStats(pipeline.inputQueue());
      }
      if (pipeline.worker() != null) {
        addThreadPoolStats(pipeline.name(), pipeline.worker());
      }
    }
    return this;
  }

  /** Adds a linebreak to the output. */
  public ProgressLoggers newLine() {
    return add(System.lineSeparator());
  }

  /** Invoke {@link #log()} at a fixed duration until {@code future} completes. */
  public void awaitAndLog(Future<?> future, Duration logInterval) {
    while (!await(future, logInterval)) {
      log();
    }
  }

  /** Returns true if the future is done, false if {@code duration} has elapsed. */
  private static boolean await(Future<?> future, Duration duration) {
    try {
      future.get(duration.toNanos(), TimeUnit.NANOSECONDS);
      return true;
    } catch (InterruptedException | ExecutionException e) {
      throw new IllegalStateException(e);
    } catch (TimeoutException e) {
      return false;
    }
  }

  private static record ProgressLogger(String name, Supplier<String> fn) {

    @Override
    public String toString() {
      return " " + name + ": " + fn.get();
    }
  }

  private static record WorkerPipelineLogger(Supplier<String> fn) {

    @Override
    public String toString() {
      return fn.get();
    }
  }
}
