package com.onthegomap.flatmap.worker;

import com.onthegomap.flatmap.monitoring.ProgressLoggers;
import com.onthegomap.flatmap.monitoring.Stats;
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
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Worker {

  private static final Logger LOGGER = LoggerFactory.getLogger(Worker.class);
  private final String prefix;
  private final CompletableFuture<?> done;

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
    stats.gauge(prefix + "_threads", threads);
    var es = Executors.newFixedThreadPool(threads, new NamedThreadFactory(prefix));
    List<CompletableFuture<?>> results = new ArrayList<>();
    for (int i = 0; i < threads; i++) {
      results.add(CompletableFuture.runAsync(() -> {
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

  public String getPrefix() {
    return prefix;
  }

  public static CompletableFuture<?> joinFutures(CompletableFuture<?>... futures) {
    return joinFutures(List.of(futures));
  }

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

  public CompletableFuture<?> done() {
    return done;
  }

  public void awaitAndLog(ProgressLoggers loggers, Duration logInterval) {
    loggers.awaitAndLog(done(), logInterval);
  }

  public void await() {
    try {
      done().get();
    } catch (ExecutionException | InterruptedException e) {
      throw new RuntimeException(e);
    }
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

  public interface RunnableThatThrows {

    void run() throws Exception;
  }
}
