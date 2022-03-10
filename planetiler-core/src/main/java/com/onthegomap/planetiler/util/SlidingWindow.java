package com.onthegomap.planetiler.util;

import com.google.common.util.concurrent.Monitor;
import java.util.concurrent.atomic.AtomicLong;

public class SlidingWindow {

  private final AtomicLong tail = new AtomicLong(0);
  private final Monitor monitor = new Monitor();
  private final long windowSize;

  public SlidingWindow(long windowSize) {
    this.windowSize = windowSize;
  }

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
