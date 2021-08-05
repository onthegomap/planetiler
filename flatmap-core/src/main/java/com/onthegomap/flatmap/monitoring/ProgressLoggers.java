package com.onthegomap.flatmap.monitoring;

import static com.onthegomap.flatmap.Format.*;

import com.graphhopper.util.Helper;
import com.onthegomap.flatmap.Format;
import com.onthegomap.flatmap.MemoryEstimator;
import com.onthegomap.flatmap.worker.WorkQueue;
import com.onthegomap.flatmap.worker.Worker;
import com.onthegomap.flatmap.worker.WorkerPipeline;
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
import java.util.function.BiFunction;
import java.util.function.DoubleFunction;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProgressLoggers {

  public static final String ANSI_RESET = "\u001B[0m";
  public static final String ANSI_RED = "\u001B[31m";
  public static final String ANSI_GREEN = "\u001B[32m";
  public static final String ANSI_YELLOW = "\u001B[33m";

  private static final Logger LOGGER = LoggerFactory.getLogger(ProgressLoggers.class);
  private final List<Object> loggers;
  private final String prefix;

  public ProgressLoggers(String prefix) {
    this.prefix = prefix;
    loggers = new ArrayList<>();
  }

  public String getLog() {
    return "[" + prefix + "]" + loggers.stream()
      .map(Object::toString)
      .collect(Collectors.joining(""));
  }

  public ProgressLoggers addRateCounter(String name, LongSupplier getValue) {
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
      return ANSI_GREEN + "[ " + formatNumeric(valueNow, true) + " " + formatNumeric(valueDiff / timeDiff, true)
        + "/s ]" + ANSI_RESET;
    }));
    return this;
  }

  public ProgressLoggers addRateCounter(String name, AtomicLong value) {
    return addRateCounter(name, value::get);
  }

  public ProgressLoggers addRatePercentCounter(String name, long total, AtomicLong value) {
    return addRatePercentCounter(name, total, value::get);
  }

  public ProgressLoggers addRatePercentCounter(String name, long total, LongSupplier getValue) {
    return addRatePercentCounter(name, total, getValue, Format::formatNumeric);
  }

  public ProgressLoggers addRatePercentBytesCounter(String name, long total, LongSupplier getValue) {
    return addRatePercentCounter(name, total, getValue, Format::formatStorage);
  }

  public ProgressLoggers addRatePercentCounter(String name, long total, LongSupplier getValue,
    BiFunction<Number, Boolean, String> format) {
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
      return ANSI_GREEN + "[ " + format.apply(valueNow, true) + " " + padLeft(formatPercent(1f * valueNow / total), 4)
        + " " + format.apply(valueDiff / timeDiff, true) + "/s ]" + ANSI_RESET;
    }));
    return this;
  }

  public ProgressLoggers addPercentCounter(String name, long total, AtomicLong getValue) {
    loggers.add(new ProgressLogger(name, () -> {
      long valueNow = getValue.get();
      return ANSI_GREEN + "[ " + padLeft("" + valueNow, 3) + " / " + padLeft("" + total, 3) + " " + padLeft(
        formatPercent(1f * valueNow / total), 4) + " ]" + ANSI_RESET;
    }));
    return this;
  }

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

  public ProgressLoggers add(Object obj) {
    loggers.add(obj);
    return this;
  }

  public ProgressLoggers addFileSize(Path file) {
    loggers.add(string(() -> {
      String bytes;
      try {
        bytes = formatBytes(Files.size(file), false);
      } catch (IOException e) {
        bytes = "-";
      }
      return " " + padRight(bytes, 5);
    }));
    return this;
  }

  public static Object string(Supplier<String> supplier) {
    return new Object() {
      @Override
      public String toString() {
        return supplier.get();
      }
    };
  }

  public ProgressLoggers addFileSize(LongSupplier longSupplier) {
    loggers.add(string(() -> " " + padRight(formatBytes(longSupplier.getAsLong(), false), 5)));
    return this;
  }

  public ProgressLoggers addProcessStats() {
    add("\n" + " ".repeat(3));
    addOptionalDeltaLogger("cpus", ProcessInfo::getProcessCpuTime, Format::formatDecimal);
    addDeltaLogger("gc", ProcessInfo::getGcTime, Format::formatPercent);
    loggers.add(new ProgressLogger("mem",
      () ->
        formatMB(Helper.getUsedMB(), false) + " / " +
          formatMB(Helper.getTotalMB(), false) +
          ProcessInfo.getMemoryUsageAfterLastGC().stream()
            .mapToObj(value -> " postGC: " + formatBytes(value, false))
            .findFirst()
            .orElse("")
    ));
    return this;
  }

  private void addOptionalDeltaLogger(String name, Supplier<Optional<Duration>> supplier,
    DoubleFunction<String> format) {
    addDeltaLogger(name, () -> supplier.get().orElse(Duration.ZERO), format);
  }

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
            long last = lastThreads.getOrDefault(thread.id(), ProcessInfo.ThreadState.DEFAULT).cpuTimeNanos();
            return padLeft(formatPercent(1d * (thread.cpuTimeNanos() - last) / timeDiff), 3);
          }).collect(Collectors.joining(" ", "(", ")"));

        lastTime.set(currentTime);
        lastThreads.putAll(newThreads);
        return (first ? "\n    " : " -> ") + name + percents;
      }));
    } catch (Throwable e) {
      // can't get CPU stats per-thread
    }
    return this;
  }

  public ProgressLoggers addThreadPoolStats(String name, Worker worker) {
    return addThreadPoolStats(name, worker.getPrefix());
  }

  private String formatBytes(long bytes, boolean pad) {
    return formatStorage(bytes, pad);
  }

  private String formatMB(long mb, boolean pad) {
    return formatStorage(mb * Helper.MB, pad);
  }

  public void log() {
    LOGGER.info(getLog());
  }

  public ProgressLoggers addInMemoryObject(String name, MemoryEstimator.HasEstimate object) {
    loggers.add(new ProgressLogger(name, () -> formatStorage(object.estimateMemoryUsageBytes(), true)));
    return this;
  }

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

  public ProgressLoggers newLine() {
    return add("\n    ");
  }

  public void awaitAndLog(Future<?> future, Duration logInterval) {
    while (!await(future, logInterval)) {
      log();
    }
  }

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
