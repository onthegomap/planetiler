package com.onthegomap.flatmap.worker;

import static com.onthegomap.flatmap.worker.Worker.joinFutures;

import com.onthegomap.flatmap.stats.ProgressLoggers;
import com.onthegomap.flatmap.stats.Stats;
import java.time.Duration;
import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;
import java.util.function.Supplier;

public record WorkerPipeline<T>(
  String name,
  WorkerPipeline<?> previous,
  WorkQueue<T> inputQueue,
  Worker worker,
  CompletableFuture<?> done
) {

  public static Empty start(String prefix, Stats stats) {
    return new Empty(prefix, stats);
  }

  public void awaitAndLog(ProgressLoggers loggers, Duration logInterval) {
    loggers.awaitAndLog(done, logInterval);
    loggers.log();
  }

  public void await() {
    try {
      done.get();
    } catch (InterruptedException | ExecutionException e) {
      throw new RuntimeException(e);
    }
  }

  public interface SourceStep<O> {

    void run(Consumer<O> next) throws Exception;
  }

  public interface WorkerStep<I, O> {

    void run(Supplier<I> prev, Consumer<O> next) throws Exception;
  }

  public interface SinkStep<I> {

    void run(Supplier<I> prev) throws Exception;
  }

  public interface Bufferable<I, O> {

    Builder<I, O> addBuffer(String name, int size, int batchSize);

    default Builder<I, O> addBuffer(String name, int size) {

      return addBuffer(name, size, 1);
    }
  }

  public static record Empty(String prefix, Stats stats) {

    public <T> Bufferable<?, T> fromGenerator(String name, SourceStep<T> producer, int threads) {
      return (queueName, size, batchSize) -> {
        var nextQueue = new WorkQueue<T>(prefix + "_" + queueName, size, batchSize, stats);
        Worker worker = new Worker(prefix + "_" + name, stats, threads,
          () -> producer.run(nextQueue.threadLocalWriter()));
        return new Builder<>(prefix, name, nextQueue, worker, stats);
      };
    }

    public <T> Bufferable<?, T> fromGenerator(String name, SourceStep<T> producer) {
      return fromGenerator(name, producer, 1);
    }

    public <T> Bufferable<?, T> readFrom(String name, Iterable<T> iterable) {
      Iterator<T> iter = iterable.iterator();
      return fromGenerator(name, next -> {
        while (iter.hasNext()) {
          next.accept(iter.next());
        }
      }, 1);
    }

    public <T> Builder<?, T> readFromTiny(String name, Collection<T> items) {
      WorkQueue<T> queue = new WorkQueue<>(prefix + "_" + name, items.size(), 1, stats);
      Consumer<T> writer = queue.threadLocalWriter();
      for (T item : items) {
        writer.accept(item);
      }
      queue.close();
      return readFromQueue(queue);
    }

    public <T> Builder<?, T> readFromQueue(WorkQueue<T> input) {
      return new Builder<>(prefix, input, stats);
    }
  }

  public static record Builder<I, O>(
    String prefix,
    String name,
    WorkerPipeline.Builder<?, I> previous,
    WorkQueue<I> inputQueue,
    WorkQueue<O> outputQueue,
    Worker worker, Stats stats
  ) {

    public Builder(String prefix, String name, WorkQueue<O> outputQueue, Worker worker, Stats stats) {
      this(prefix, name, null, null, outputQueue, worker, stats);
    }

    public Builder(String prefix, WorkQueue<O> outputQueue, Stats stats) {
      this(prefix, null, outputQueue, null, stats);
    }

    public <O2> Bufferable<O, O2> addWorker(String name, int threads, WorkerStep<O, O2> step) {
      Builder<I, O> curr = this;
      return (queueName, size, batchSize) -> {
        var nextOutputQueue = new WorkQueue<O2>(prefix + "_" + queueName, size, batchSize, stats);
        var worker = new Worker(prefix + "_" + name, stats, threads,
          () -> step.run(outputQueue.threadLocalReader(), nextOutputQueue.threadLocalWriter()));
        return new Builder<>(prefix, name, curr, outputQueue, nextOutputQueue, worker, stats);
      };
    }

    private WorkerPipeline<I> build() {
      var previousPipeline = previous == null || previous.worker == null ? null : previous.build();
      var doneFuture = worker != null ? worker.done() : CompletableFuture.completedFuture(true);
      if (previousPipeline != null) {
        doneFuture = joinFutures(doneFuture, previousPipeline.done);
      }
      if (worker != null && outputQueue != null) {
        doneFuture = doneFuture.thenRun(outputQueue::close);
      }
      return new WorkerPipeline<>(name, previousPipeline, inputQueue, worker, doneFuture);
    }

    public WorkerPipeline<O> sinkTo(String name, int threads, SinkStep<O> step) {
      var previousPipeline = build();
      var worker = new Worker(prefix + "_" + name, stats, threads, () -> step.run(outputQueue.threadLocalReader()));
      var doneFuture = joinFutures(worker.done(), previousPipeline.done);
      return new WorkerPipeline<>(name, previousPipeline, outputQueue, worker, doneFuture);
    }

    public WorkerPipeline<O> sinkToConsumer(String name, int threads, Consumer<O> step) {
      return sinkTo(name, threads, (prev) -> {
        O item;
        while ((item = prev.get()) != null) {
          step.accept(item);
        }
      });
    }
  }

}
