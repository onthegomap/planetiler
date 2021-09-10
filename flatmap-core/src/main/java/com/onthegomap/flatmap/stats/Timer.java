package com.onthegomap.flatmap.stats;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Measures the amount of wall and CPU time that a task takes.
 */
@ThreadSafe
public class Timer {

  private final ProcessTime start;
  private volatile ProcessTime end;

  private Timer() {
    start = ProcessTime.now();
  }

  public static Timer start() {
    return new Timer();
  }

  /**
   * Sets the end time to now, and makes {@link #running()} return false. Calling multiple times will extend the end
   * time.
   */
  public Timer stop() {
    synchronized (this) {
      end = ProcessTime.now();
    }
    return this;
  }

  /** Returns {@code false} if {@link #stop()} has been called. */
  public boolean running() {
    synchronized (this) {
      return end == null;
    }
  }

  /** Returns the time from start to now if the task is still running, or start to end if it has finished. */
  public ProcessTime elapsed() {
    synchronized (this) {
      return (end == null ? ProcessTime.now() : end).minus(start);
    }
  }

  @Override
  public String toString() {
    return elapsed().toString();
  }
}
