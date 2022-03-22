package com.onthegomap.planetiler.reader.osm;

import static io.prometheus.client.Collector.NANOSECONDS_PER_SECOND;

import com.onthegomap.planetiler.stats.Counter;
import com.onthegomap.planetiler.stats.Timer;
import com.onthegomap.planetiler.util.Format;
import com.onthegomap.planetiler.worker.RunnableThatThrows;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Phaser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Coordinates multiple workers processing OSM elements sequentially:
 * <ul>
 * <li>Ensure nodes processed first, then ways, then relations</li>
 * <li>Keep a count of elements processed</li>
 * <li>Lets workers wait to start an element type until all workers are finished with the previous element type
 * ({@link ForWorker#arriveAndWaitForOthers(Phase)})</li>
 * <li>Log when starting a new phase, including how long the previous phase took and the number of elements per
 * second</li>
 * </ul>
 * Each worker should call {@link #forWorker()} to get a handle for that worker to coordinate with others.
 */
class OsmPhaser {

  private final Format FORMAT = Format.defaultInstance();
  private final Logger LOGGER = LoggerFactory.getLogger(OsmPhaser.class);
  private final ConcurrentHashMap<Phase, Timer> startTimes = new ConcurrentHashMap<>();
  private final ConcurrentHashMap<Phase, Counter.MultiThreadCounter> counts = new ConcurrentHashMap<>();
  private final Phaser phaser = new Phaser() {
    @Override
    protected boolean onAdvance(int phase, int registeredParties) {
      allWorkersFinished(Phase.from(phase));
      return super.onAdvance(phase, registeredParties);
    }
  };
  private volatile int registered = 0;

  /**
   * Creates a new phaser expecting a certain number of workers to register.
   */
  OsmPhaser(int workers) {
    phaser.bulkRegister(workers);
  }

  long nodes() {
    return getCount(Phase.NODES);
  }

  long ways() {
    return getCount(Phase.WAYS);
  }

  long relations() {
    return getCount(Phase.RELATIONS);
  }

  private void workerStarted(Phase phase) {
    startTimes.computeIfAbsent(phase, p -> Timer.start());
  }

  private void allWorkersFinished(Phase phase) {
    var timer = startTimes.get(phase);
    if (timer != null) {
      timer.stop();
    }
    String summary = getSummary(phase);
    if (summary != null) {
      LOGGER.info("Finished " + getSummary(phase));
    }
  }

  private long getCount(Phase phase) {
    var counter = counts.get(phase);
    return counter == null ? 0 : counter.get();
  }

  private String getSummary(Phase phase) {
    String result = null;
    var timer = startTimes.get(phase);
    long count = getCount(phase);
    if (timer != null && count > 0) {
      double rate = count * NANOSECONDS_PER_SECOND / timer.elapsed().wall().toNanos();
      result = phase + ": " + FORMAT.integer(count) + " (" + FORMAT.numeric(rate) + "/s) in " + timer;
    }
    return result;
  }

  /**
   * Prints how many elements were processed per phase, how long they took, and how many elements per second were
   * processed.
   */
  public void printSummary() {
    LOGGER.debug("  " + getSummary(Phase.NODES));
    LOGGER.debug("  " + getSummary(Phase.WAYS));
    LOGGER.debug("  " + getSummary(Phase.RELATIONS));
  }

  /**
   * Indicate that {@code workers} workers will be registering eventually, so that if one starts early it doesn't
   * advance through phases before it knows about the other workers.
   */
  public void registerWorkers(int workers) {
    phaser.bulkRegister(workers);
  }

  /**
   * Returns a new {@link ForWorker} handle for a worker to use to coordinate with other workers.
   * <p>
   * If {@link #registerWorkers(int)} has not been called yet, or called with a smaller number, this will automatically
   * register another worker.
   */
  public ForWorker forWorker() {
    return new ForWorker();
  }

  Phase getPhase() {
    return Phase.from(phaser.getPhase());
  }

  public enum Phase {
    /** Before processing any OSM elements */
    BEGIN(0),
    NODES(1),
    WAYS(2),
    RELATIONS(3),
    /** After finished processing all OSM elements */
    DONE(4);

    private final int number;

    Phase(int number) {
      this.number = number;
    }

    private static Phase from(int number) {
      for (var phase : values()) {
        if (number == phase.number) {
          return phase;
        }
      }
      return number < 0 ? Phase.BEGIN : Phase.DONE;
    }

    public Phase next() {
      return from(number + 1);
    }

    @Override
    public String toString() {
      return super.toString().toLowerCase(Locale.ROOT);
    }

    public Phase prev() {
      return from(number - 1);
    }
  }

  /** Handle for a worker to use to coordinate with other workers processing OSM elements. */
  class ForWorker implements AutoCloseable {
    /*
     * This worker keeps track of the current phase in not-thread-safe variables, and when the phase
     * changes coordinates with other threads through the Phaser.
     */

    private final EnumMap<Phase, List<RunnableThatThrows>> finishActions = new EnumMap<>(Phase.class);
    private Phase currentPhase = Phase.BEGIN;
    private Counter counterForPhase;
    // we don't increment a counter until after an element is processed, so need to keep track of if
    // we skipped the current phase so that we don't increment the counter when the phase is finished
    private boolean skippedPhase = true;

    private ForWorker() {
      synchronized (phaser) {
        registered++;
        if (registered > phaser.getRegisteredParties()) {
          phaser.register();
        }
      }
    }

    /** Register {@code action} to run after this worker finished {@code phase}. */
    public ForWorker whenWorkerFinishes(Phase phase, RunnableThatThrows action) {
      finishActions.computeIfAbsent(phase, p -> new ArrayList<>()).add(action);
      return this;
    }

    private void advance(Phase newPhase, boolean waitForOthers, boolean isSkip) {
      if (currentPhase == newPhase) {
        // normal case - still working on same phase
        counterForPhase.inc();
      } else if (newPhase.number < currentPhase.number) {
        throw new IllegalStateException(
          "Elements must be sorted with nodes first, then ways, then relations. Encountered " + newPhase + " after " +
            currentPhase);
      } else {
        // advance to next phase
        // but first, check if we skipped over some phases
        while (currentPhase.next() != newPhase) {
          advance(currentPhase.next(), true, true);
        }
        for (var action : finishActions.getOrDefault(currentPhase, List.of())) {
          action.runAndWrapException();
        }
        // increment the counter for the last element processed from the last phase, unless
        // the last phase was BEGIN, or we skipped over the last phase.
        if (counterForPhase != null && !skippedPhase) {
          counterForPhase.inc();
        }
        skippedPhase = isSkip;

        currentPhase = currentPhase.next();
        counterForPhase = counts.computeIfAbsent(currentPhase, p -> Counter.newMultiThreadCounter()).counterForThread();

        // don't let a worker move to next phase if other workers aren't even on that phase yet
        if (phaser.getPhase() < currentPhase.number - 1) {
          waitForAllToFinish(currentPhase.prev().prev());
        }
        phaser.arrive();
        if (waitForOthers) {
          waitForAllToFinish(currentPhase.prev());
        }
        workerStarted(currentPhase);

        // don't increment the counter since we want the count to go up after each element
        // was processed (not before).
      }
    }

    private void waitForAllToFinish(Phase phase) {
      try {
        phaser.awaitAdvanceInterruptibly(phase.number);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException(e);
      }
    }

    /**
     * Start processing an element in {@code phase} without waiting for other workers to start on the phase.
     * <p>
     * If this is the first element in that phase, then the previous phase is marked as done which triggers any handler
     * registered through {@link #whenWorkerFinishes(Phase, RunnableThatThrows)}. If this was the last worker on the
     * previous phase, then the overall phase will advance into {@code phase} now.
     * <p>
     * This could block if we need to wait for other workers to get to the previous phase before we leave it.
     *
     * @throws RuntimeException if the thread is interrupted while waiting
     */
    public void arrive(Phase phase) {
      advance(phase, false, false);
    }

    /**
     * Wait for all workers to get to this point before start to processing the first element in {@code phase}.
     *
     * @throws RuntimeException if the thread is interrupted while waiting
     */
    public void arriveAndWaitForOthers(Phase phase) {
      advance(phase, true, false);
    }

    @Override
    public void close() {
      advance(Phase.DONE, false, false);
    }
  }
}
