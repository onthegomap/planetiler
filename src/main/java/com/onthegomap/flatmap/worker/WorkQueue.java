package com.onthegomap.flatmap.worker;

import com.onthegomap.flatmap.monitoring.Stats;
import java.io.Closeable;
import java.util.ArrayDeque;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Supplier;

public class WorkQueue<T> implements Closeable, Supplier<T>, Consumer<T> {

  private final ThreadLocal<Queue<T>> itemWriteBatchProvider = new ThreadLocal<>();
  private final ThreadLocal<Queue<T>> itemReadBatchProvider = new ThreadLocal<>();
  private final BlockingQueue<Queue<T>> itemQueue;
  private final int batchSize;
  private final ConcurrentHashMap<Long, Queue<T>> queues = new ConcurrentHashMap<>();
  private final int pendingBatchesCapacity;
  private volatile boolean hasIncomingData = true;
  private final AtomicInteger pendingCount = new AtomicInteger(0);

  public WorkQueue(String name, int capacity, int maxBatch, Stats stats) {
    this.pendingBatchesCapacity = capacity / maxBatch;
    this.batchSize = maxBatch;
    itemQueue = new ArrayBlockingQueue<>(pendingBatchesCapacity);
  }

  @Override
  public void close() {
    for (Queue<T> q : queues.values()) {
      try {
        if (!q.isEmpty()) {
          itemQueue.put(q);
        }
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
    hasIncomingData = false;
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
  }

  private void flushWrites() {
    Queue<T> writeBatch = itemWriteBatchProvider.get();
    if (writeBatch != null && !writeBatch.isEmpty()) {
      try {
        itemWriteBatchProvider.set(null);
        queues.remove(Thread.currentThread().getId());
        // blocks if full
        if (!itemQueue.offer(writeBatch)) {
          itemQueue.put(writeBatch);
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
    }

    T result = itemBatch == null ? null : itemBatch.poll();
    if (result != null) {
      pendingCount.decrementAndGet();
    }
    return result;
  }

  public int getPending() {
    return pendingCount.get();
  }

  public int getCapacity() {
    return pendingBatchesCapacity * batchSize;
  }
}

