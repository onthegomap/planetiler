package com.onthegomap.flatmap.worker;

import com.onthegomap.flatmap.ProgressLoggers;
import com.onthegomap.flatmap.stats.Stats;
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

  public void awaitAndLog(ProgressLoggers loggers, long logIntervalSeconds) {
    if (previous != null) {
      previous.awaitAndLog(loggers, logIntervalSeconds);
    } else { // producer is responsible for closing the initial input queue
      inputQueue.close();
    }
    worker.awaitAndLog(loggers, logIntervalSeconds);
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
        var nextQueue = new WorkQueue<T>(prefix, queueName, size, batchSize, stats);
        Worker worker = new Worker(prefix, name, stats, threads, () -> producer.run(nextQueue));
        return new Builder<>(prefix, name, nextQueue, worker, stats);
      };
    }

    public <T> Bufferable<?, T> fromGenerator(String name, SourceStep<T> producer) {
      return fromGenerator(name, producer, 1);
    }

    public <T> Bufferable<?, T> readFromIterator(String name, Iterator<T> iter) {
      return fromGenerator(name, next -> {
        while (iter.hasNext()) {
          next.accept(iter.next());
        }
      }, 1);
    }

    public <T> Builder<?, T> readFromQueue(WorkQueue<T> input) {
      return new Builder<>(input, stats);
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

    public Builder(WorkQueue<O> outputQueue, Stats stats) {
      this(null, null, outputQueue, null, stats);
    }

    public <O2> Bufferable<O, O2> addWorker(String name, int threads, WorkerStep<O, O2> step) {
      Builder<I, O> curr = this;
      return (queueName, size, batchSize) -> {
        var nextOutputQueue = new WorkQueue<O2>(prefix, queueName, size, batchSize, stats);
        var worker = new Worker(prefix, name, stats, threads, () -> step.run(outputQueue, nextOutputQueue));
        return new Builder<>(prefix, name, curr, outputQueue, nextOutputQueue, worker, stats);
      };
    }

    private Topology<I> build() {
      var previousTopology = previous == null || previous.worker == null ? null : previous.build();
      return new Topology<>(name, previousTopology, inputQueue, worker);
    }

    public Topology<O> sinkTo(String name, int threads, SinkStep<O> step) {
      var previousTopology = previous.build();
      var worker = new Worker(prefix, name, stats, threads, () -> step.run(outputQueue));
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
