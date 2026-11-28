package com.onthegomap.planetiler.stats;

import static com.onthegomap.planetiler.util.LogUtil.UNTIMED_LOGGER;

import com.onthegomap.planetiler.util.Format;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;
import net.jcip.annotations.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;

/**
 * A registry of tasks that are being timed.
 */
@ThreadSafe
public class Timers {

  private static final Logger LOGGER = LoggerFactory.getLogger(Timers.class);
  private static final Format FORMAT = Format.defaultInstance();
  private final Map<String, Stage> timers = Collections.synchronizedMap(new LinkedHashMap<>());
  private final AtomicReference<Stage> currentStage = new AtomicReference<>();

  private final Timer sinceCreated = Timer.start();
  private volatile Duration logInterval = Duration.ofSeconds(10);

  /**
   * Sets the time between progress lines. Stages that take at least that long log when they finish, shorter ones only
   * at debug level since they printed no progress and the summary table already shows their time.
   */
  public void setLogInterval(Duration logInterval) {
    this.logInterval = logInterval;
  }

  /**
   * Logs the time since this registry was created, then a table with the wall time, CPU time and average cores used by
   * each stage that logged when it started, plus each stage's worker threads when debug logging is on.
   */
  public void printSummary() {
    if (UNTIMED_LOGGER.isInfoEnabled()) {
      UNTIMED_LOGGER.info("");
      UNTIMED_LOGGER.info("Finished in {}{}", sinceCreated.elapsed(), stageTable(UNTIMED_LOGGER.isDebugEnabled()));
    }
  }

  private String stageTable(boolean withWorkers) {
    var stages = all().entrySet().stream().filter(entry -> entry.getValue().logged()).toList();
    int nameWidth =
      Math.max("stage".length(), stages.stream().mapToInt(entry -> entry.getKey().length()).max().orElse(0));
    StringBuilder result = new StringBuilder();
    result.append(stageTableRow(nameWidth, "stage", "time", "cpu", "avg cores"));
    for (var entry : stages) {
      String name = entry.getKey();
      ProcessTime elapsed = entry.getValue().timer.elapsed();
      // Average cores over a few milliseconds is noise, so very short stages leave it blank.
      String avgCores = elapsed.wall().compareTo(Duration.ofMillis(50)) < 0 ? "" :
        elapsed.cpu().map(cpu -> FORMAT.decimal(cpu.toNanos() * 1d / elapsed.wall().toNanos())).orElse("");
      result.append(stageTableRow(
        nameWidth,
        name,
        FORMAT.duration(elapsed.wall()),
        elapsed.cpu().map(FORMAT::duration).orElse("-"),
        avgCores));
      if (withWorkers) {
        for (String detail : getStageDetails(name, true)) {
          result.append(System.lineSeparator()).append("\t  ").append(detail);
        }
      }
    }
    return result.toString();
  }

  private static String stageTableRow(int nameWidth, String name, String time, String cpu, String avgCores) {
    return System.lineSeparator() + "\t" + Format.padRight(name, nameWidth) + Format.padLeft(time, 7) +
      (Format.padLeft(cpu, 7) + "   " + avgCores).stripTrailing();
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
    return startTimer(name, true);
  }

  public Finishable startTimer(String name, boolean logStart) {
    return startTimer(name, logStart ? "Starting..." : null);
  }

  /**
   * Starts a timer for stage {@code name} that logs {@code startMessage} when it starts and "Finished in" when it
   * finishes, or neither if {@code startMessage} is null.
   */
  public Finishable startTimer(String name, String startMessage) {
    boolean logStart = startMessage != null;
    Timer timer = Timer.start();
    Stage stage = new Stage(timer, logStart);
    timers.put(name, stage);
    Stage last = currentStage.getAndSet(stage);
    if (logStart) {
      LOGGER.info(startMessage);
    }
    return new Finishable() {
      @Override
      public void stop() {
        timer.stop();
        if (logStart) {
          // skip "Finished" if it ran fast enough that no progress was printed (<10s)
          boolean printedProgress = timer.elapsed().wall().compareTo(logInterval) >= 0;
          LOGGER.atLevel(printedProgress ? Level.INFO : Level.DEBUG).log("Finished in {}", timer);
          if (LOGGER.isDebugEnabled()) {
            for (var details : getStageDetails(name, true)) {
              LOGGER.debug("  {}", details);
            }
          }
        }
        currentStage.set(last);
      }

      @Override
      public ProcessTime elapsed() {
        return timer.elapsed();
      }
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

    ProcessTime elapsed();
  }

  record ThreadInfo(ProcessInfo.ThreadState state, String prefix, Duration elapsed) {}

  /**
   * A timed task, {@code logged} if it logged when it started and finished, and so shows up in the summary.
   */
  record Stage(Timer timer, boolean logged, List<ThreadInfo> threadStats) {

    Stage(Timer timer, boolean logged) {
      this(timer, logged, new CopyOnWriteArrayList<>());
    }
  }
}
