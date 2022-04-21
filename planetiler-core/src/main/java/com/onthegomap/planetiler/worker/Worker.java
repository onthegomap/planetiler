package com.onthegomap.planetiler.worker;

import static com.onthegomap.planetiler.util.Exceptions.throwFatalException;

import com.onthegomap.planetiler.stats.ProgressLoggers;
import com.onthegomap.planetiler.stats.Stats;
import com.onthegomap.planetiler.util.LogUtil;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Executes a task in parallel across multiple threads.
 */
public class Worker {

  private static final Logger LOGGER = LoggerFactory.getLogger(Worker.class);
  private final String prefix;
  private final CompletableFuture<Void> done;
  private static final AtomicBoolean firstWorkerDied = new AtomicBoolean(false);

  /**
   * Constructs a new worker and immediately starts {@code threads} thread all running {@code task}.
   *
   * @param prefix  string ID to add to logs and stats
   * @param stats   stats collector for this thread pool
   * @param threads number of parallel threads to run {@code task} in
   * @param task    the work to do in each thread
   */
  @SuppressWarnings("java:S1181")
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
          long start = System.nanoTime();
          task.run();
          stats.timers().finishedWorker(prefix, Duration.ofNanos(System.nanoTime() - start));
        } catch (Throwable e) {
          System.err.println("Worker " + id + " died");
          // when one worker dies it may close resources causing others to die as well, so only log the first
          if (firstWorkerDied.compareAndSet(false, true)) {
            e.printStackTrace();
          }
          throwFatalException(e);
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
  public static CompletableFuture<Void> joinFutures(CompletableFuture<?>... futures) {
    return joinFutures(List.of(futures));
  }

  /**
   * Returns a future that completes successfully when all {@code futures} complete, or fails immediately when the first
   * one fails.
   */
  public static CompletableFuture<Void> joinFutures(Collection<CompletableFuture<?>> futures) {
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

  public String getPrefix() {
    return prefix;
  }

  public CompletableFuture<Void> done() {
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
      throwFatalException(e);
    }
  }

  /** A thread factory that prepends {@code name-} to all thread names. */
  private static class NamedThreadFactory implements ThreadFactory {

    private final ThreadGroup group;
    private final AtomicInteger threadNumber = new AtomicInteger(1);
    private final String namePrefix;

    private NamedThreadFactory(String name) {
      group = Thread.currentThread().getThreadGroup();
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
