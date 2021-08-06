package com.onthegomap.flatmap.worker;

import com.onthegomap.flatmap.stats.Counter;
import com.onthegomap.flatmap.stats.Stats;
import java.util.ArrayDeque;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Supplier;

public class WorkQueue<T> implements AutoCloseable, Supplier<T>, Consumer<T> {

  private final ThreadLocal<WriterForThread> writerProvider = ThreadLocal.withInitial(WriterForThread::new);
  private final ThreadLocal<ReaderForThread> readerProvider = ThreadLocal.withInitial(ReaderForThread::new);
  private final BlockingQueue<Queue<T>> itemQueue;
  private final int batchSize;
  private final List<WriterForThread> writers = new CopyOnWriteArrayList<>();
  private final List<ReaderForThread> readers = new CopyOnWriteArrayList<>();
  private final int pendingBatchesCapacity;
  private final Counter.MultiThreadCounter enqueueCountStatAll;
  private final Counter.MultiThreadCounter enqueueBlockTimeNanosAll;
  private final Counter.MultiThreadCounter dequeueCountStatAll;
  private final Counter.MultiThreadCounter dequeueBlockTimeNanosAll;
  private volatile boolean hasIncomingData = true;
  private final Counter.MultiThreadCounter pendingCountAll = Counter.newMultiThreadCounter();

  public WorkQueue(String name, int capacity, int maxBatch, Stats stats) {
    this.pendingBatchesCapacity = capacity / maxBatch;
    this.batchSize = maxBatch;
    itemQueue = new ArrayBlockingQueue<>(pendingBatchesCapacity);

    stats.gauge(name + "_blocking_queue_capacity", () -> pendingBatchesCapacity);
    stats.gauge(name + "_blocking_queue_size", itemQueue::size);
    stats.gauge(name + "_capacity", this::getCapacity);
    stats.gauge(name + "_size", this::getPending);

    this.enqueueCountStatAll = stats.longCounter(name + "_enqueue_count");
    this.enqueueBlockTimeNanosAll = stats.nanoCounter(name + "_enqueue_block_time_seconds");
    this.dequeueCountStatAll = stats.longCounter(name + "_dequeue_count");
    this.dequeueBlockTimeNanosAll = stats.nanoCounter(name + "_dequeue_block_time_seconds");
  }

  @Override
  public void close() {
    try {
      for (var writer : writers) {
        var q = writer.writeBatchRef.get();
        if (q != null && !q.isEmpty()) {
          itemQueue.put(q);
        }
      }
      hasIncomingData = false;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public Consumer<T> threadLocalWriter() {
    return writerProvider.get();
  }

  public Supplier<T> threadLocalReader() {
    return readerProvider.get();
  }

  @Override
  public void accept(T item) {
    writerProvider.get().accept(item);
  }

  @Override
  public T get() {
    return readerProvider.get().get();
  }

  private class WriterForThread implements Consumer<T> {

    AtomicReference<Queue<T>> writeBatchRef = new AtomicReference<>(null);
    Queue<T> writeBatch = null;
    Counter pendingCount = pendingCountAll.counterForThread();
    Counter enqueueCountStat = enqueueCountStatAll.counterForThread();
    Counter enqueueBlockTimeNanos = enqueueBlockTimeNanosAll.counterForThread();

    WriterForThread() {
      writers.add(this);
    }

    @Override
    public void accept(T item) {
      // past 4-8 concurrent writers, start getting lock contention adding to the blocking queue so add to the
      // queue in less frequent, larger batches
      if (writeBatch == null) {
        writeBatch = new ArrayDeque<>(batchSize);
        writeBatchRef.set(writeBatch);
      }

      writeBatch.offer(item);
      pendingCount.inc();

      if (writeBatch.size() >= batchSize) {
        flushWrites();
      }
      enqueueCountStat.inc();
    }

    private void flushWrites() {
      if (writeBatch != null && !writeBatch.isEmpty()) {
        try {
          Queue<T> oldWriteBatch = writeBatch;
          writeBatch = null;
          writeBatchRef.set(null);
          // blocks if full
          if (!itemQueue.offer(oldWriteBatch)) {
            long start = System.nanoTime();
            itemQueue.put(oldWriteBatch);
            enqueueBlockTimeNanos.incBy(System.nanoTime() - start);
          }
        } catch (InterruptedException ex) {
          throw new RuntimeException(ex);
        }
      }
    }
  }

  private class ReaderForThread implements Supplier<T> {

    Queue<T> readBatch = null;
    Counter dequeueBlockTimeNanos = dequeueBlockTimeNanosAll.counterForThread();
    Counter pendingCount = pendingCountAll.counterForThread();
    Counter dequeueCountStat = dequeueCountStatAll.counterForThread();

    ReaderForThread() {
      readers.add(this);
    }

    @Override
    public T get() {
      Queue<T> itemBatch = readBatch;

      if (itemBatch == null || itemBatch.isEmpty()) {
        long start = System.nanoTime();
        do {
          if (!hasIncomingData && itemQueue.isEmpty()) {
            break;
          }

          if ((itemBatch = itemQueue.poll()) == null) {
            try {
              itemBatch = itemQueue.poll(100, TimeUnit.MILLISECONDS);
              if (itemBatch != null) {
                break;
              }
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
              break;// signal EOF
            }
          }
        } while (itemBatch == null);
        readBatch = itemBatch;
        dequeueBlockTimeNanos.incBy(System.nanoTime() - start);
      }

      T result = itemBatch == null ? null : itemBatch.poll();
      if (result != null) {
        pendingCount.incBy(-1);
      }
      dequeueCountStat.inc();
      return result;
    }
  }

  public int getPending() {
    return (int) pendingCountAll.get();
  }

  public int getCapacity() {
    // actual queue can hold more than the specified capacity because each writer and reader may have a batch they are
    // working on that is outside of the queue
    return (pendingBatchesCapacity + writers.size() + readers.size()) * batchSize;
  }
}

