package com.onthegomap.planetiler.worker;

import com.onthegomap.planetiler.stats.Counter;
import com.onthegomap.planetiler.stats.Stats;
import java.util.ArrayDeque;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * A high-performance blocking queue to hand off work from producing threads to consuming threads.
 * <p>
 * Wraps a standard {@link BlockingDeque}, with a few customizations:
 * <ul>
 *   <li>items are buffered into configurable-sized batches before putting on the actual queue to reduce contention</li>
 *   <li>writers can mark the queue "finished" with {@link #close()} and readers will get {@code null} when there are
 *   no more items to read</li>
 * </ul>
 * <p>
 * Once a thread starts reading from this queue, it needs to finish otherwise all items might not be read.
 *
 * @param <T> the type of elements held in this queue
 */
public class WorkQueue<T> implements AutoCloseable, Supplier<T>, Consumer<T> {

  private final BlockingQueue<Queue<T>> itemQueue;
  private final int batchSize;
  private final List<WriterForThread> writers = new CopyOnWriteArrayList<>();
  private final ThreadLocal<WriterForThread> writerProvider = ThreadLocal.withInitial(WriterForThread::new);
  private final List<ReaderForThread> readers = new CopyOnWriteArrayList<>();
  private final ThreadLocal<ReaderForThread> readerProvider = ThreadLocal.withInitial(ReaderForThread::new);
  private final int pendingBatchesCapacity;
  private final Counter.MultiThreadCounter enqueueCountStatAll;
  private final Counter.MultiThreadCounter enqueueBlockTimeNanosAll;
  private final Counter.MultiThreadCounter dequeueCountStatAll;
  private final Counter.MultiThreadCounter dequeueBlockTimeNanosAll;
  private final Counter.MultiThreadCounter pendingCountAll = Counter.newMultiThreadCounter();
  private volatile boolean hasIncomingData = true;

  /**
   * @param name     ID to prepend to stats generated about this queue
   * @param capacity maximum number of pending items that can be held in the queue
   * @param maxBatch batch size to buffer elements into before handing off to the blocking queue
   * @param stats    stats to monitor this with
   */
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

  /** Returns a writer optimized to accept items from a single thread. */
  public Consumer<T> threadLocalWriter() {
    return writerProvider.get();
  }

  /** Returns a reader optimized to produce items for a single thread. */
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

  /**
   * Returns the number of enqueued items that have not been dequeued yet.
   * <p>
   * NOTE: this may be larger than the initial capacity because each writer thread can buffer items into a batch and
   * each reader thread might be reading items from a batch.
   */
  public int getPending() {
    return (int) pendingCountAll.get();
  }

  /**
   * Returns the total number of items that can be pending.
   * <p>
   * This will be larger than the initial capacity because each writer thread can buffer items into a batch and each
   * reader thread can read items from a batch.
   */
  public int getCapacity() {
    // actual queue can hold more than the specified capacity because each writer and reader may have a batch they are
    // working on that is outside the queue
    return (pendingBatchesCapacity + writers.size() + readers.size()) * batchSize;
  }

  /** Caches thread-local values so that a single thread can accept new items without having to do thread-local lookups. */
  private class WriterForThread implements Consumer<T> {

    final AtomicReference<Queue<T>> writeBatchRef = new AtomicReference<>(null);
    Queue<T> writeBatch = null;
    final Counter pendingCount = pendingCountAll.counterForThread();
    final Counter enqueueCountStat = enqueueCountStatAll.counterForThread();
    final Counter enqueueBlockTimeNanos = enqueueBlockTimeNanosAll.counterForThread();

    private WriterForThread() {
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

  /** Caches thread-local values so that a single thread can read new items without having to do thread-local lookups. */
  private class ReaderForThread implements Supplier<T> {

    Queue<T> readBatch = null;
    final Counter dequeueBlockTimeNanos = dequeueBlockTimeNanosAll.counterForThread();
    final Counter pendingCount = pendingCountAll.counterForThread();
    final Counter dequeueCountStat = dequeueCountStatAll.counterForThread();

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
}

