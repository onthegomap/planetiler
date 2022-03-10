package com.onthegomap.planetiler.util;

import com.google.common.util.concurrent.Monitor;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Lets multiple threads work on a sliding window of values.
 *
 * Calls to {@link #waitUntilInsideWindow(long)} will block until {@link #advanceTail(long)} is within a given range
 * from the new value being requested.
 */
public class SlidingWindow {

  private final AtomicLong tail = new AtomicLong(0);
  private final Monitor monitor = new Monitor();
  private final long windowSize;

  public SlidingWindow(long windowSize) {
    this.windowSize = windowSize;
  }

  /**
   * Moves the current value for the tail to {@code to}, unblocking any thread waiting on moving the head to
   * {@code to + windowSize}.
   */
  public void advanceTail(long to) {
    monitor.enter();
    try {
      if (to < tail.get()) {
        throw new IllegalStateException("Tried to move sliding window tail backwards from " + tail + " to " + to);
      }
      tail.set(to);
    } finally {
      monitor.leave();
    }
  }

  /** Blocks until another thread moves the tail to at least {@code to - windowSize}. */
  public void waitUntilInsideWindow(long to) {
    try {
      monitor.enterWhen(monitor.newGuard(() -> to - tail.longValue() < windowSize));
      monitor.leave();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    }
  }
}
