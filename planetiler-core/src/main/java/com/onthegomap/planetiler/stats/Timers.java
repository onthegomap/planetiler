package com.onthegomap.planetiler.stats;

import com.onthegomap.planetiler.util.Format;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.concurrent.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A registry of tasks that are being timed.
 */
@ThreadSafe
public class Timers {

  private static final Logger LOGGER = LoggerFactory.getLogger(Stats.InMemory.class);
  private static final Format FORMAT = Format.defaultInstance();
  private final Map<String, Stage> timers = Collections.synchronizedMap(new LinkedHashMap<>());
  private final AtomicReference<Stage> currentStage = new AtomicReference<>();

  public void printSummary() {
    int maxLength = (int) all().keySet().stream().mapToLong(String::length).max().orElse(0);
    for (var entry : all().entrySet()) {
      String name = entry.getKey();
      var elapsed = entry.getValue().timer.elapsed();
      LOGGER.info("\t" + Format.padRight(name, maxLength) + " " + elapsed);
      if (elapsed.wall().compareTo(Duration.ofSeconds(1)) > 0) {
        for (String detail : getStageDetails(name, true)) {
          LOGGER.info("\t  " + detail);
        }
      }
    }
  }

  private List<String> getStageDetails(String name, boolean pad) {
    List<String> resultList = new ArrayList<>();
    Stage stage = timers.get(name);
    var elapsed = stage.timer.elapsed();
    List<String> threads = stage.threadStats.stream().map(d -> d.prefix).distinct().toList();
    int maxLength = !pad ? 0 : (int) (threads.stream()
      .map(n -> n.replace(name + "_", ""))
      .mapToLong(String::length)
      .max().orElse(0)) + 1;
    for (String thread : threads) {
      StringBuilder result = new StringBuilder();
      List<ThreadInfo> threadStates = stage.threadStats.stream()
        .filter(t -> t.prefix.equals(thread))
        .toList();
      int num = threadStates.size();
      ProcessInfo.ThreadState sum = threadStates.stream()
        .map(d -> d.state)
        .reduce(ProcessInfo.ThreadState.DEFAULT, ProcessInfo.ThreadState::plus);
      double totalNanos = elapsed.wall().multipliedBy(num).toNanos();
      result.append(Format.padRight(thread.replace(name + "_", ""), maxLength))
        .append(Format.padLeft(Integer.toString(num), 2))
        .append("x(")
        .append(FORMAT.percent(sum.cpuTime().toNanos() / totalNanos))
        .append(" ")
        .append(FORMAT.duration(sum.cpuTime().dividedBy(num)));

      Duration systemTime = sum.cpuTime().minus(sum.userTime()).dividedBy(num);
      if (systemTime.compareTo(Duration.ofSeconds(1)) > 0) {
        result.append(" sys:").append(FORMAT.duration(systemTime));
      }
      Duration blockTime = sum.blocking().dividedBy(num);
      if (blockTime.compareTo(Duration.ofSeconds(1)) > 0) {
        result.append(" block:").append(FORMAT.duration(blockTime));
      }
      Duration waitTime = sum.waiting().dividedBy(num);
      if (waitTime.compareTo(Duration.ofSeconds(1)) > 0) {
        result.append(" wait:").append(FORMAT.duration(waitTime));
      }
      Duration totalThreadElapsedTime = threadStates.stream().map(d -> d.elapsed)
        .reduce(Duration::plus)
        .orElse(Duration.ZERO)
        .dividedBy(num);
      Duration doneTime = elapsed.wall().minus(totalThreadElapsedTime);
      if (doneTime.compareTo(Duration.ofSeconds(1)) > 0) {
        result.append(" done:").append(FORMAT.duration(doneTime));
      }
      result.append(")");
      resultList.add(result.toString());
    }
    return resultList;
  }

  public Finishable startTimer(String name) {
    Timer timer = Timer.start();
    Stage stage = new Stage(timer);
    timers.put(name, stage);
    Stage last = currentStage.getAndSet(stage);
    LOGGER.info("");
    LOGGER.info("Starting...");
    return () -> {
      LOGGER.info("Finished in " + timers.get(name).timer.stop());
      for (var details : getStageDetails(name, true)) {
        LOGGER.info("  " + details);
      }
      currentStage.set(last);
    };
  }

  public void finishedWorker(String prefix, Duration elapsed) {
    Stage stage = currentStage.get();
    if (stage != null) {
      stage.threadStats.add(new ThreadInfo(ProcessInfo.getCurrentThreadState(), prefix, elapsed));
    }
  }

  /** Returns a snapshot of all timers currently running. Will not reflect timers that start after it's called. */
  public Map<String, Stage> all() {
    synchronized (timers) {
      return new LinkedHashMap<>(timers);
    }
  }

  /** A handle that callers can use to indicate a task has finished. */
  public interface Finishable {

    void stop();
  }

  record ThreadInfo(ProcessInfo.ThreadState state, String prefix, Duration elapsed) {}

  record Stage(Timer timer, List<ThreadInfo> threadStats) {

    Stage(Timer timer) {
      this(timer, new CopyOnWriteArrayList<>());
    }
  }
}
