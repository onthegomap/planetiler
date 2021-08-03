package com.onthegomap.flatmap.worker;

import com.onthegomap.flatmap.monitoring.ProgressLoggers;
import com.onthegomap.flatmap.monitoring.Stats;
import java.time.Duration;
import java.util.Collection;
import java.util.Iterator;
import java.util.function.Consumer;
import java.util.function.Supplier;

public record Topology<T>(
  String name,
  com.onthegomap.flatmap.worker.Topology<?> previous,
  WorkQueue<T> inputQueue,
  Worker worker
) {

  public static Empty start(String prefix, Stats stats) {
    return new Empty(prefix, stats);
  }

  // track time since last log and stagger initial log interval for each step to keep logs
  // coming at consistent intervals
  private void doAwaitAndLog(ProgressLoggers loggers, Duration logInterval, long startNanos) {
    if (previous != null) {
      previous.doAwaitAndLog(loggers, logInterval, startNanos);
      if (inputQueue != null) {
        inputQueue.close();
      }
    }
    if (worker != null) {
      long elapsedSoFar = System.nanoTime() - startNanos;
      Duration sinceLastLog = Duration.ofNanos(elapsedSoFar % logInterval.toNanos());
      Duration untilNextLog = logInterval.minus(sinceLastLog);
      worker.awaitAndLog(loggers, untilNextLog, logInterval);
    }
  }

  public void awaitAndLog(ProgressLoggers loggers, Duration logInterval) {
    doAwaitAndLog(loggers, logInterval, System.nanoTime());
    loggers.log();
  }

  public void await() {
    if (previous != null) {
      previous.await();
      if (inputQueue != null) {
        inputQueue.close();
      }
    }
    if (worker != null) {
      worker.await();
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
      return readFromQueue(queue);
    }

    public <T> Builder<?, T> readFromQueue(WorkQueue<T> input) {
      return new Builder<>(prefix, input, stats);
    }
  }

  public static record Builder<I, O>(
    String prefix,
    String name,
    Topology.Builder<?, I> previous,
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

    private Topology<I> build() {
      var previousTopology = previous == null || previous.worker == null ? null : previous.build();
      return new Topology<>(name, previousTopology, inputQueue, worker);
    }

    public Topology<O> sinkTo(String name, int threads, SinkStep<O> step) {
      var previousTopology = build();
      var worker = new Worker(prefix + "_" + name, stats, threads, () -> step.run(outputQueue.threadLocalReader()));
      return new Topology<>(name, previousTopology, outputQueue, worker);
    }

    public Topology<O> sinkToConsumer(String name, int threads, Consumer<O> step) {
      return sinkTo(name, threads, (prev) -> {
        O item;
        while ((item = prev.get()) != null) {
          step.accept(item);
        }
      });
    }
  }

}
