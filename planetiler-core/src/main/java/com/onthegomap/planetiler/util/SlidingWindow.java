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

//  private final CopyOnWriteArrayList<AtomicInteger> workers = new CopyOnWriteArrayList<>();
//  private final ThreadLocal<AtomicInteger> currentIndex = ThreadLocal.withInitial(() -> {
//    var result = new AtomicInteger(0);
//    workers.add(result);
//    return result;
//  });
//  final Lock lock = new ReentrantLock();
//  final Condition hasSpace = lock.newCondition();
//  private final int limit;
//
//  public SlidingWindow(int limit) {
//    this.limit = limit;
//  }
//
//  private synchronized int min() {
//    return workers.stream().mapToInt(AtomicInteger::intValue).min().orElseThrow();
//  }
//
//  private synchronized int max() {
//    return workers.stream().mapToInt(AtomicInteger::intValue).max().orElseThrow();
//  }
//
//  public void acquireIndex(int value) {
//    AtomicInteger old = currentIndex.get();
//    int oldMin
//  }
//
//  public void release() {
//    AtomicInteger old = currentIndex.get();
//    notify();
//  }
}
