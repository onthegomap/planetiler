package com.onthegomap.planetiler.worker;

import com.onthegomap.planetiler.collection.IterableOnce;
import java.util.ArrayDeque;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * A high-performance blocking queue to hand off work from a single producing thread to single
 * consuming thread.
 *
 * <p>Each element has a weight and each batch has a target maximum weight to put more lightweight
 * objects in a batch or fewer heavy-weight ones.
 *
 * @param <T> the type of elements held in this queue
 */
public class WeightedHandoffQueue<T> implements AutoCloseable, IterableOnce<T> {

  private final Queue<T> DONE = new ArrayDeque<>(0);
  private final BlockingQueue<Queue<T>> itemQueue;
  private final int writeLimit;
  private boolean done = false;
  private int writeCost = 0;
  Queue<T> writeBatch = null;
  Queue<T> readBatch = null;

  /**
   * Creates a new {@code WeightedHandoffQueue} with {@code outer} maximum number of pending batches
   * and {@code inner} maximum batch weight.
   */
  public WeightedHandoffQueue(int outer, int inner) {
    this.writeLimit = inner;
    itemQueue = new ArrayBlockingQueue<>(outer);
  }

  @Override
  public void close() {
    try {
      flushWrites();
      itemQueue.put(DONE);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public void accept(T item, int cost) {
    if (writeBatch == null) {
      writeBatch = new ArrayDeque<>(writeLimit / 2);
    }

    writeCost += cost;
    writeBatch.offer(item);

    if (writeCost >= writeLimit) {
      flushWrites();
    }
  }

  private void flushWrites() {
    if (writeBatch != null && !writeBatch.isEmpty()) {
      try {
        Queue<T> oldWriteBatch = writeBatch;
        writeBatch = null;
        writeCost = 0;
        itemQueue.put(oldWriteBatch);
      } catch (InterruptedException ex) {
        throw new RuntimeException(ex);
      }
    }
  }

  @Override
  public T get() {
    Queue<T> itemBatch = readBatch;

    if (itemBatch == null || itemBatch.isEmpty()) {
      do {
        if (done && itemQueue.isEmpty()) {
          break;
        }

        if ((itemBatch = itemQueue.poll()) == null) {
          try {
            itemBatch = itemQueue.poll(100, TimeUnit.MILLISECONDS);
            if (itemBatch != null) {
              if (itemBatch == DONE) {
                done = true;
              }
              break;
            }
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            break; // signal EOF
          }
        } else if (itemBatch == DONE) {
          done = true;
        }
      } while (itemBatch == null);
      readBatch = itemBatch;
    }

    return itemBatch == null ? null : itemBatch.poll();
  }
}
