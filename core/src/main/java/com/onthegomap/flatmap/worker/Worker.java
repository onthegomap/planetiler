package com.onthegomap.flatmap.worker;

import com.onthegomap.flatmap.monitoring.ProgressLoggers;
import com.onthegomap.flatmap.monitoring.Stats;
import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Worker {

  private static final Logger LOGGER = LoggerFactory.getLogger(Worker.class);
  private final ExecutorService es;
  private final String prefix;
  private final Stats stats;

  private static class NamedThreadFactory implements ThreadFactory {

    private final ThreadGroup group;
    private final AtomicInteger threadNumber = new AtomicInteger(1);
    private final String namePrefix;

    private NamedThreadFactory(String name) {
      SecurityManager s = System.getSecurityManager();
      group = (s != null) ? s.getThreadGroup() :
        Thread.currentThread().getThreadGroup();
      namePrefix = name + "-";
    }

    @Override
    public Thread newThread(@NotNull Runnable r) {
      Thread t = new Thread(group, r,
        namePrefix + threadNumber.getAndIncrement(),
        0);
      if (!t.isDaemon()) {
        t.setDaemon(true);
      }
      if (t.getPriority() != Thread.NORM_PRIORITY) {
        t.setPriority(Thread.NORM_PRIORITY);
      }
      return t;
    }
  }

  public Worker(String prefix, Stats stats, int threads, RunnableThatThrows task) {
    this.prefix = prefix;
    this.stats = stats;
    stats.gauge(prefix + "_threads", threads);
    es = Executors.newFixedThreadPool(threads, new NamedThreadFactory(prefix));
    for (int i = 0; i < threads; i++) {
      es.submit(() -> {
        String id = Thread.currentThread().getName();
        LOGGER.trace("Starting worker");
        try {
          task.run();
        } catch (Throwable e) {
          System.err.println("Worker " + id + " died");
          e.printStackTrace();
          System.exit(1);
        } finally {
          LOGGER.trace("Finished worker");
        }
      });
    }
    es.shutdown();
  }

  public String getPrefix() {
    return prefix;
  }

  public void awaitAndLog(ProgressLoggers loggers, Duration initialLogInterval, Duration logInterval) {
    try {
      if (!es.awaitTermination(initialLogInterval.toNanos(), TimeUnit.NANOSECONDS)) {
        loggers.log();
        while (!es.awaitTermination(logInterval.toNanos(), TimeUnit.NANOSECONDS)) {
          loggers.log();
        }
      }
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  public void await() {
    try {
      es.awaitTermination(365, TimeUnit.DAYS);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  public interface RunnableThatThrows {

    void run() throws Exception;
  }
}
