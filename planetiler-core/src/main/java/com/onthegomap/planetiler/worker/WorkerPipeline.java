package com.onthegomap.planetiler.worker;

import static com.onthegomap.planetiler.util.Exceptions.throwFatalException;
import static com.onthegomap.planetiler.worker.Worker.joinFutures;

import com.onthegomap.planetiler.collection.IterableOnce;
import com.onthegomap.planetiler.stats.ProgressLoggers;
import com.onthegomap.planetiler.stats.Stats;
import java.time.Duration;
import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;

/**
 * A mini-framework for chaining sequential steps that run in dedicated threads with a queue between each.
 * <p>
 * For example:
 *
 * {@snippet :
 * WorkerPipeline.start("name", stats)
 *   .readFrom("reader", List.of(1, 2, 3))
 *   .addBuffer("reader_queue", 10)
 *   .addWorker("process", 2, (i, next) -> next.accept(doExpensiveWork(i)))
 *   .addBuffer("writer_queue", 10)
 *   .sinkToConsumer("writer", 1, result -> writeToDisk(result))
 *   .await();
 * }
 * <p>
 * NOTE: to do any forking/joining, you must construct and wire-up queues and each sequence of steps manually.
 *
 * @param <T> input type of this pipeline
 */
public record WorkerPipeline<T>(
  String name,
  WorkerPipeline<?> previous,
  WorkQueue<T> inputQueue,
  Worker worker,
  CompletableFuture<Void> done
) {
  /*
   * Empty/Bufferable/Builder are used to provide a fluent API for building a model of the steps to run (and keep
   * pointers to workers and queues) and Builder.build converts to the top-level WorkerPipeline that clients
   * can use to wait on results.
   */

  /** Returns a new pipeline builder where all worker and queue names will start with {@code prefix_}. */
  public static Empty start(String prefix, Stats stats) {
    return new Empty(prefix, stats);
  }

  /**
   * Blocks until all work has been completed by all steps of this pipeline, logging progress at a fixed {@code
   * logInterval}.
   *
   * @throws RuntimeException if interrupted or if any of the threads die with an exception.
   */
  public void awaitAndLog(ProgressLoggers loggers, Duration logInterval) {
    loggers.awaitAndLog(done, logInterval);
  }

  /**
   * Blocks until all work has been completed by all steps of this pipeline.
   *
   * @throws RuntimeException if interrupted or if any of the threads die with an exception.
   */
  public void await() {
    try {
      done.get();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throwFatalException(e);
    } catch (ExecutionException e) {
      throwFatalException(e);
    }
  }

  /**
   * Work that happens in a thread at the start of a pipeline to provide the initial elements to process.
   *
   * @param <O> type of items that this step will emit
   */
  @FunctionalInterface
  public interface SourceStep<O> {

    /**
     * Called inside each worker thread to emit elements until there are no more, then return.
     *
     * @param next call {@code next.accept} to pass elements to the next step of the pipeline
     * @throws Exception if an error occurs, will be rethrown by {@link #await()} as a {@link RuntimeException}
     */
    void run(Consumer<O> next) throws Exception;
  }

  /**
   * Work that happens in a thread in the middle of a pipeline that accepts elements from previous steps and emits
   * elements to the next step.
   *
   * @param <I> type of items that this step consumes
   * @param <O> type of items that this step emits
   */
  @FunctionalInterface
  public interface WorkerStep<I, O> {

    /**
     * Called inside each worker thread to process elements until there are no more, then return.
     *
     * @param prev get elements from the previous step using {@code prev.get}, will return null when there are no more
     *             elements to process
     * @param next call {@code next.accept} to pass items to the next step of the pipeline
     * @throws Exception if an error occurs, will be rethrown by {@link #await()} as a {@link RuntimeException}
     */
    void run(IterableOnce<I> prev, Consumer<O> next) throws Exception;
  }

  /**
   * Work that happens in a thread at the end of a pipeline that accepts elements from the previous step.
   *
   * @param <I> type of items that this step consumes
   */
  @FunctionalInterface
  public interface SinkStep<I> {

    /**
     * Called inside each worker thread to consume elements until there are no more, then return.
     *
     * @param prev get elements from the previous step using {@code prev.get}, will return null when there are no more
     *             elements to process
     * @throws Exception if an error occurs, will be rethrown by {@link #await()} as a {@link RuntimeException}
     */
    void run(IterableOnce<I> prev) throws Exception;
  }

  /**
   * An intermediate step while building a pipeline that requires the user to add a queue to buffer items before adding
   * the next step.
   *
   * @param <E> type of elements coming out of the previous step that need to be stored in a queue
   */
  public interface Bufferable<E> {

    /**
     * Adds a {@link WorkQueue} that groups items into batches before enqueueing them to reduce contention when many
     * threads are reading or writing to the queue simultaneously.
     */
    Builder<E> addBuffer(String name, int size, int batchSize);

    /**
     * Adds a {@link WorkQueue} with batching disabled.
     */
    default Builder<E> addBuffer(String name, int size) {
      return addBuffer(name, size, 1);
    }
  }

  /** Builder for a new topology that does not yet have any steps. */
  public record Empty(String prefix, Stats stats) {

    /**
     * Adds an initial step that runs {@code producer} in {@code threads} worker threads to produce items for this
     * queue.
     */
    public <T> Bufferable<T> fromGenerator(String name, SourceStep<T> producer, int threads) {
      return (queueName, size, batchSize) -> {
        var nextQueue = new WorkQueue<T>(prefix + "_" + queueName, size, batchSize, stats);
        Worker worker = new Worker(prefix + "_" + name, stats, threads,
          () -> producer.run(nextQueue.threadLocalWriter()));
        return new Builder<>(prefix, name, nextQueue, worker, stats);
      };
    }

    /**
     * Adds an initial step that runs {@code producer} in 1 worker thread to produce items for this queue.
     */
    public <T> Bufferable<T> fromGenerator(String name, SourceStep<T> producer) {
      return fromGenerator(name, producer, 1);
    }

    /**
     * Adds an initial step that reads all items from {@code iterable} in a single worker thread to produce items for
     * this queue.
     */
    public <T> Bufferable<T> readFrom(String name, Iterable<T> iterable) {
      Iterator<T> iter = iterable.iterator();
      return fromGenerator(name, next -> {
        while (iter.hasNext()) {
          next.accept(iter.next());
        }
      }, 1);
    }

    /**
     * Populates an initial queue with {@code items} for subsequent steps to process but does not kick off a worker
     * thread.
     */
    public <T> Builder<T> readFromTiny(String name, Collection<T> items) {
      WorkQueue<T> queue = new WorkQueue<>(prefix + "_" + name, items.size(), 1, stats);
      Consumer<T> writer = queue.threadLocalWriter();
      for (T item : items) {
        writer.accept(item);
      }
      queue.close();
      return readFromQueue(queue);
    }

    /**
     * Starts the pipeline from a queue that is populated externally, and does not kick off any threads.
     */
    public <T> Builder<T> readFromQueue(WorkQueue<T> input) {
      return new Builder<>(prefix, input, stats);
    }
  }

  /**
   * A step while building a pipeline that needs a subsequent step to process elements.
   *
   * @param <O> type of elements that the next step must process
   */
  public record Builder<O>(
    String prefix,
    String name,
    // keep track of previous elements so that build can wire-up the computation graph
    WorkerPipeline.Builder<?> previous,
    WorkQueue<?> inputQueue,
    WorkQueue<O> outputQueue,
    Worker worker,
    Stats stats
  ) {

    private Builder(String prefix, String name, WorkQueue<O> outputQueue, Worker worker, Stats stats) {
      this(prefix, name, null, null, outputQueue, worker, stats);
    }

    private Builder(String prefix, WorkQueue<O> outputQueue, Stats stats) {
      this(prefix, null, outputQueue, null, stats);
    }

    /**
     * Runs {@code step} simultaneously in {@code threads} threads which consumes items and emits new ones that must be
     * buffered.
     *
     * @param <O2> type of element that this step emits
     */
    public <O2> Bufferable<O2> addWorker(String name, int threads, WorkerStep<O, O2> step) {
      Builder<O> curr = this;
      return (queueName, size, batchSize) -> {
        var nextOutputQueue = new WorkQueue<O2>(prefix + "_" + queueName, size, batchSize, stats);
        var worker = new Worker(prefix + "_" + name, stats, threads,
          () -> step.run(outputQueue.threadLocalReader(), nextOutputQueue.threadLocalWriter()));
        return new Builder<>(prefix, name, curr, outputQueue, nextOutputQueue, worker, stats);
      };
    }

    private WorkerPipeline<?> build() {
      var previousPipeline = previous == null || previous.worker == null ? null : previous.build();
      CompletableFuture<Void> doneFuture =
        worker != null ? worker.done() : CompletableFuture.completedFuture(null);
      if (previousPipeline != null) {
        doneFuture = joinFutures(doneFuture, previousPipeline.done);
      }
      if (worker != null && outputQueue != null) {
        doneFuture = doneFuture.thenRun(outputQueue::close);
      }
      return new WorkerPipeline<>(name, previousPipeline, inputQueue, worker, doneFuture);
    }

    /**
     * Runs {@code step} simultaneously in {@code threads} threads that consumes items but does not emit any.
     */
    public WorkerPipeline<O> sinkTo(String name, int threads, SinkStep<O> step) {
      var previousPipeline = build();
      var worker = new Worker(prefix + "_" + name, stats, threads, () -> step.run(outputQueue.threadLocalReader()));
      var doneFuture = joinFutures(worker.done(), previousPipeline.done);
      return new WorkerPipeline<>(name, previousPipeline, outputQueue, worker, doneFuture);
    }

    /**
     * Runs {@code threads} simultaneous worker threads that consume items from previous step and invoke {@code
     * consumer.accept} for each one.
     */
    public WorkerPipeline<O> sinkToConsumer(String name, int threads, Consumer<O> consumer) {
      return sinkTo(name, threads, (prev) -> {
        for (O item : prev) {
          consumer.accept(item);
        }
      });
    }
  }
}
