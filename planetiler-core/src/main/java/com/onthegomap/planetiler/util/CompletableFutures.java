package com.onthegomap.planetiler.util;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Consumer;
import java.util.function.Function;

/** Utilities for waiting on {@link CompletableFuture CompletableFutures}. */
public class CompletableFutures {
  /** A future that has already completed with {@code null}. */
  public static final CompletableFuture<?> NULL = CompletableFuture.completedFuture(null);

  private CompletableFutures() {}

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

  /**
   * Runs {@code task} on {@code executor} like
   * {@link CompletableFuture#supplyAsync(java.util.function.Supplier, Executor)}, except that {@code task} may throw
   * checked exceptions.
   * <p>
   * The future fails with whatever {@code task} throws.
   */
  public static <T> CompletableFuture<T> supplyAsync(Callable<T> task, Executor executor) {
    return CompletableFuture.supplyAsync(() -> {
      try {
        return task.call();
      } catch (RuntimeException e) {
        throw e;
      } catch (Exception e) {
        throw new CompletionException(e);
      }
    }, executor);
  }

  /**
   * Waits for {@code future} and returns its result, or throws what it failed with without the
   * {@link ExecutionException} around it.
   * <p>
   * A {@link RuntimeException} or {@link Error} gets thrown as is, checked exceptions the way
   * {@link Exceptions#throwFatalException(Throwable)} throws them (an {@link IOException} as an
   * {@link UncheckedIOException}); see {@link #awaitIO(CompletableFuture)} to get IOExceptions back.
   */
  public static <T> T await(CompletableFuture<T> future) {
    try {
      return future.get();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return Exceptions.throwFatalException(e);
    } catch (ExecutionException e) {
      return Exceptions.throwFatalException(e.getCause());
    }
  }

  /**
   * Waits for {@code future} like {@link #await(CompletableFuture)} but throws an {@link IOException} it failed with
   * (also when wrapped in an {@link UncheckedIOException}) as a checked {@link IOException}.
   */
  public static <T> T awaitIO(CompletableFuture<T> future) throws IOException {
    try {
      return await(future);
    } catch (UncheckedIOException e) {
      return Exceptions.throwAsIOException(e);
    }
  }

  /**
   * Waits for all {@code futures}, which each open a resource, and returns the resources in the same order.
   * <p>
   * If any fail, this still waits for all of them, closes the ones that opened, and throws the failure of the first one
   * that failed in list order, like {@link #awaitIO(CompletableFuture)}.
   */
  public static <T extends Closeable> List<T> joinAllOrClose(List<CompletableFuture<T>> futures) throws IOException {
    List<T> result = new ArrayList<>();
    Throwable error = null;
    for (var future : futures) {
      try {
        result.add(future.join());
      } catch (CompletionException e) {
        error = error == null ? e.getCause() : error;
      }
    }
    if (error != null) {
      for (var opened : result) {
        try {
          opened.close();
        } catch (IOException e) {
          error.addSuppressed(e);
        }
      }
      Exceptions.throwAsIOException(error);
    }
    return result;
  }

  /**
   * Passes {@code items} to {@code next} in the order they become ready instead of the order they come in.
   * <p>
   * {@code start} gets called on each item in order and returns a future that completes, normally or exceptionally,
   * once the item is ready. At most {@code window} items are started but not passed to {@code next} yet at once. Items
   * that are ready right away get passed on in order.
   */
  public static <T> void inCompletionOrder(Iterable<T> items, int window, Function<T, CompletableFuture<?>> start,
    Consumer<T> next) throws InterruptedException {
    if (window < 1) {
      throw new IllegalArgumentException("window must be at least 1, was " + window);
    }
    var ready = new LinkedBlockingQueue<T>();
    Consumer<T> startItem = item -> start.apply(item).whenComplete((_, _) -> ready.add(item));
    var iterator = items.iterator();
    int inProgress = 0;
    // initially, start up to `window` futures
    while (inProgress < window && iterator.hasNext()) {
      startItem.accept(iterator.next());
      inProgress++;
    }
    // then each time any one finishes, pass it on and start the next one
    while (iterator.hasNext()) {
      next.accept(ready.take());
      startItem.accept(iterator.next());
    }
    // then after all started, drain the queue
    while (inProgress > 0) {
      next.accept(ready.take());
      inProgress--;
    }
  }
}
