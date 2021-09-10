package com.onthegomap.flatmap.worker;

import com.onthegomap.flatmap.stats.ProgressLoggers;
import com.onthegomap.flatmap.stats.Stats;
import com.onthegomap.flatmap.util.LogUtil;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Executes a task in parallel across multiple threads.
 */
public class Worker {

  private static final Logger LOGGER = LoggerFactory.getLogger(Worker.class);
  private final String prefix;
  private final CompletableFuture<?> done;

  /**
   * Constructs a new worker and immediately starts {@code threads} thread all running {@code task}.
   *
   * @param prefix  string ID to add to logs and stats
   * @param stats   stats collector for this thread pool
   * @param threads number of parallel threads to run {@code task} in
   * @param task    the work to do in each thread
   */
  public Worker(String prefix, Stats stats, int threads, RunnableThatThrows task) {
    this.prefix = prefix;
    stats.gauge(prefix + "_threads", threads);
    var es = Executors.newFixedThreadPool(threads, new NamedThreadFactory(prefix));
    String parentStage = LogUtil.getStage();
    List<CompletableFuture<?>> results = new ArrayList<>();
    for (int i = 0; i < threads; i++) {
      results.add(CompletableFuture.runAsync(() -> {
        LogUtil.setStage(parentStage, prefix);
        String id = Thread.currentThread().getName();
        LOGGER.trace("Starting worker");
        try {
          task.run();
        } catch (Throwable e) {
          System.err.println("Worker " + id + " died");
          throwRuntimeException(e);
        } finally {
          LOGGER.trace("Finished worker");
        }
      }, es));
    }
    es.shutdown();
    done = joinFutures(results);
  }

  /**
   * Returns a future that completes successfully when all {@code futures} complete, or fails immediately when the first
   * one fails.
   */
  public static CompletableFuture<?> joinFutures(CompletableFuture<?>... futures) {
    return joinFutures(List.of(futures));
  }

  /**
   * Returns a future that completes successfully when all {@code futures} complete, or fails immediately when the first
   * one fails.
   */
  public static CompletableFuture<?> joinFutures(Collection<CompletableFuture<?>> futures) {
    CompletableFuture<Void> result = CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new));
    // fail fast on exceptions
    for (CompletableFuture<?> f : futures) {
      f.whenComplete((res, ex) -> {
        if (ex != null) {
          result.completeExceptionally(ex);
          futures.forEach(other -> other.cancel(true));
        }
      });
    }
    return result;
  }

  private static void throwRuntimeException(Throwable exception) {
    if (exception instanceof RuntimeException runtimeException) {
      throw runtimeException;
    } else if (exception instanceof IOException ioe) {
      throw new UncheckedIOException(ioe);
    } else if (exception instanceof Error error) {
      throw error;
    } else if (exception instanceof InterruptedException) {
      Thread.currentThread().interrupt();
    }
    throw new RuntimeException(exception);
  }

  public String getPrefix() {
    return prefix;
  }

  public CompletableFuture<?> done() {
    return done;
  }

  /**
   * Blocks until all tasks are complete, invoking {@link ProgressLoggers#log()} at a fixed duration until this task
   * completes.
   *
   * @throws RuntimeException if interrupted or if one of the threads throws.
   */
  public void awaitAndLog(ProgressLoggers loggers, Duration logInterval) {
    loggers.awaitAndLog(done(), logInterval);
  }

  /**
   * Blocks until all tasks are complete.
   *
   * @throws RuntimeException if interrupted or if one of the threads throws.
   */
  public void await() {
    try {
      done().get();
    } catch (ExecutionException | InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  /** A thread factory that prepends {@code name-} to all thread names. */
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
    public Thread newThread(Runnable r) {
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
}
