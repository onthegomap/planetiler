package com.onthegomap.planetiler.util;

import com.onthegomap.planetiler.worker.RunnableThatThrows;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Waits for multiple threads to get to a certain point in parallel work, similar to
 * {@link java.util.concurrent.CyclicBarrier} but can only be used once.
 */
public class SyncPoint {
  private final CountDownLatch latch;
  private final AtomicInteger numRegistered = new AtomicInteger();
  private final int limit;

  /** Creates a new sync point that waits for {@code workers} to finish. */
  public SyncPoint(int workers) {
    this.limit = workers;
    this.latch = new CountDownLatch(workers);
  }

  /** Returns a handle for workers to mark their work as finished, or to wait for others. */
  public Finisher newFinisher() {
    return newFinisher(() -> {
    });
  }

  /** Returns a handle for workers to mark their work as finished, or to wait for others. */
  public Finisher newFinisher(RunnableThatThrows action) {
    return limit == 0 ? new NoopFinisher(action) : new RealFinisher(action);
  }

  /** A handle for a single thread. */
  public interface Finisher extends AutoCloseable {
    /** Marks this worker as finished, handles deduplicating multiple calls. */
    void finish();

    /** Waits for all other workers to finish. */
    void await();

    /** Returns {@code true} if this worker (not others) has finished. */
    boolean isFinished();

    @Override
    default void close() {
      finish();
    }
  }

  /** Dummy finisher that always lets {@link #await()} continue. */
  private static class NoopFinisher implements Finisher {

    private final RunnableThatThrows action;
    boolean done = false;

    public NoopFinisher(RunnableThatThrows action) {
      this.action = action;
    }

    @Override
    public void finish() {
      if (!done) {
        action.runAndWrapException();
        done = true;
      }
    }

    @Override
    public void await() {}

    @Override
    public boolean isFinished() {
      return done;
    }
  }

  /** Real finisher that waits for all workers to finish before {@link #await()} continues. */
  private class RealFinisher implements Finisher {

    private final RunnableThatThrows action;
    boolean done = false;
    boolean allDone = false;

    private RealFinisher(RunnableThatThrows action) {
      if (numRegistered.incrementAndGet() > limit) {
        throw new IllegalStateException("Tried to register " + numRegistered + " finishers but limit was " + limit);
      }
      this.action = action;
    }

    @Override
    public void finish() {
      if (!done) {
        latch.countDown();
        done = true;
        action.runAndWrapException();
      }
    }

    @Override
    public void await() {
      if (!allDone) {
        if (!done) {
          throw new IllegalStateException("Waiting to be done, but this worker not marked as done yet");
        }
        try {
          latch.await();
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new RuntimeException(e);
        }
        allDone = true;
      }
    }

    @Override
    public boolean isFinished() {
      return done;
    }
  }
}
