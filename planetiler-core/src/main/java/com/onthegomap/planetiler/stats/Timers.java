package com.onthegomap.planetiler.stats;

import com.onthegomap.planetiler.util.Format;
import java.time.Duration;
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

  record ThreadInfo(ProcessInfo.ThreadState state, String prefix) {}

  record Stage(Timer timer, List<ThreadInfo> threadStats) {

    Stage(Timer timer) {
      this(timer, new CopyOnWriteArrayList<>());
    }
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(Stats.InMemory.class);
  private static final Format FORMAT = Format.defaultInstance();
  private final Map<String, Stage> timers = Collections.synchronizedMap(new LinkedHashMap<>());
  private final AtomicReference<Stage> currentStage = new AtomicReference<>();

  public void printSummary() {
    for (var entry : all().entrySet()) {
      String name = entry.getKey();
      var elapsed = entry.getValue().timer.elapsed();
      LOGGER.info("\t" + name + "\t" + elapsed);
      printStageDetails(name, "\t  ");
    }
  }

  private void printStageDetails(String name, String prefix) {
    Stage stage = timers.get(name);
    var elapsed = stage.timer.elapsed();
    List<String> threads = stage.threadStats.stream().map(d -> d.prefix).distinct().toList();
    for (String thread : threads) {
      List<ProcessInfo.ThreadState> threadStates = stage.threadStats.stream().filter(t -> t.prefix.equals(thread))
        .map(t -> t.state).toList();
      ProcessInfo.ThreadState sum = threadStates.stream().reduce(ProcessInfo.ThreadState.DEFAULT,
        ProcessInfo.ThreadState::sum);
      double totalNanos = elapsed.wall().multipliedBy(threadStates.size()).toNanos();

      LOGGER.debug(prefix + thread.replace(name + "_", "") + " x" + threadStates.size() + "\t" +
        FORMAT.duration(sum.cpuTime()) +
        " (" + FORMAT.percent(sum.cpuTime().toNanos() / totalNanos) + ")" +
        (sum.blocking().compareTo(Duration.ofSeconds(1)) > 0 ? " blocked:" + FORMAT.duration(sum.blocking()) : "") +
        (sum.waiting().compareTo(Duration.ofSeconds(1)) > 0 ? " waiting:" + FORMAT.duration(sum.waiting()) : "")
      );
    }
  }

  public Finishable startTimer(String name) {
    Timer timer = Timer.start();
    Stage stage = new Stage(timer);
    timers.put(name, stage);
    Stage last = currentStage.getAndSet(stage);
    System.out.println();
    LOGGER.info("Starting...");
    return () -> {
      LOGGER.info("Finished in " + timers.get(name).timer.stop());
      printStageDetails(name, "  ");
      currentStage.set(last);
    };
  }

  public void finishedWorker(String prefix) {
    Stage stage = currentStage.get();
    if (stage != null) {
      stage.threadStats.add(new ThreadInfo(ProcessInfo.getCurrentThreadStats(), prefix));
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
}
