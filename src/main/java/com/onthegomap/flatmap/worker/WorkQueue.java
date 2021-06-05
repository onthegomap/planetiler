package com.onthegomap.flatmap.worker;

import com.onthegomap.flatmap.monitoring.Stats;
import java.util.ArrayDeque;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Supplier;

public class WorkQueue<T> implements AutoCloseable, Supplier<T>, Consumer<T> {

  private final ThreadLocal<Queue<T>> itemWriteBatchProvider = new ThreadLocal<>();
  private final ThreadLocal<Queue<T>> itemReadBatchProvider = new ThreadLocal<>();
  private final BlockingQueue<Queue<T>> itemQueue;
  private final int batchSize;
  private final ConcurrentHashMap<Long, Queue<T>> queues = new ConcurrentHashMap<>();
  private final int pendingBatchesCapacity;
  private final Stats.StatCounter enqueueCountStat;
  private final Stats.StatCounter enqueueBlockTimeNanos;
  private final Stats.StatCounter dequeueCountStat;
  private final Stats.StatCounter dequeueBlockTimeNanos;
  private volatile boolean hasIncomingData = true;
  private final AtomicInteger pendingCount = new AtomicInteger(0);

  public WorkQueue(String name, int capacity, int maxBatch, Stats stats) {
    this.pendingBatchesCapacity = capacity / maxBatch;
    this.batchSize = maxBatch;
    itemQueue = new ArrayBlockingQueue<>(pendingBatchesCapacity);

    stats.gauge(name + "_blocking_queue_capacity", () -> pendingBatchesCapacity);
    stats.gauge(name + "_blocking_queue_size", itemQueue::size);
    stats.gauge(name + "_capacity", this::getCapacity);
    stats.gauge(name + "_size", this::getPending);

    this.enqueueCountStat = stats.longCounter(name + "_enqueue_count");
    this.enqueueBlockTimeNanos = stats.nanoCounter(name + "_enqueue_block_time_seconds");
    this.dequeueCountStat = stats.longCounter(name + "_dequeue_count");
    this.dequeueBlockTimeNanos = stats.nanoCounter(name + "_dequeue_block_time_seconds");
  }

  @Override
  public void close() {
    try {
      for (Queue<T> q : queues.values()) {
        if (!q.isEmpty()) {
          itemQueue.put(q);
        }
      }
      hasIncomingData = false;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void accept(T item) {
    // past 4-8 concurrent writers, start getting lock contention adding to the blocking queue so add to the
    // queue in lass frequent, larger batches
    Queue<T> writeBatch = itemWriteBatchProvider.get();
    if (writeBatch == null) {
      itemWriteBatchProvider.set(writeBatch = new ArrayDeque<>(batchSize));
      queues.put(Thread.currentThread().getId(), writeBatch);
    }

    writeBatch.offer(item);
    pendingCount.incrementAndGet();

    if (writeBatch.size() >= batchSize) {
      flushWrites();
    }
    enqueueCountStat.inc();
  }

  private void flushWrites() {
    Queue<T> writeBatch = itemWriteBatchProvider.get();
    if (writeBatch != null && !writeBatch.isEmpty()) {
      try {
        itemWriteBatchProvider.set(null);
        queues.remove(Thread.currentThread().getId());
        // blocks if full
        if (!itemQueue.offer(writeBatch)) {
          long start = System.nanoTime();
          itemQueue.put(writeBatch);
          enqueueBlockTimeNanos.inc(System.nanoTime() - start);
        }
      } catch (InterruptedException ex) {
        throw new RuntimeException(ex);
      }
    }
  }

  @Override
  public T get() {
    Queue<T> itemBatch = itemReadBatchProvider.get();

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
      itemReadBatchProvider.set(itemBatch);
      dequeueBlockTimeNanos.inc(System.nanoTime() - start);
    }

    T result = itemBatch == null ? null : itemBatch.poll();
    if (result != null) {
      pendingCount.decrementAndGet();
    }
    dequeueCountStat.inc();
    return result;
  }

  public int getPending() {
    return pendingCount.get();
  }

  public int getCapacity() {
    return pendingBatchesCapacity * batchSize;
  }
}

