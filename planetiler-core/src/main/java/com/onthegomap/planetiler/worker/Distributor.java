package com.onthegomap.planetiler.worker;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import javax.annotation.concurrent.NotThreadSafe;
import javax.annotation.concurrent.ThreadSafe;

/**
 * Redistributes work among worker threads when some finish early.
 * <p>
 * When a group of worker threads are processing large blocks, some may finish early, resulting in idle time at the end
 * waiting for the "long pole in the tent" to finish:
 *
 * <pre>{@code
 *          busy         idle | done
 * worker1: ===========>xxxxxx|
 * worker2: ===============>xx|
 * worker3: =================>|
 * worker4: =============>xxxx|
 * }</pre>
 * <p>
 * This utility wraps the operation to perform on each element and then works through items in 3 phases:
 *
 * <ol>
 *   <li>If all threads are still busy, process it in the same thread</li>
 *   <li>If some threads are done, enqueue the item onto a work queue (but if it is full, just process it in the same thread)</li>
 *   <li>When the thread is done processing input elements, then process items off of the work queue until it is empty and all other workers are finished</li>
 * </ol>
 *
 * @param <T> The type of element being processed
 */
@ThreadSafe
public class Distributor<T> {

  private final AtomicInteger done = new AtomicInteger();
  private final AtomicInteger working = new AtomicInteger();
  private final ArrayBlockingQueue<T> pending;

  private Distributor(int capacity) {
    pending = new ArrayBlockingQueue<>(capacity);
  }

  /** Returns a new {@code Distributor} that can hold up to {@code capacity} pending elements. */
  public static <T> Distributor<T> createWithCapacity(int capacity) {
    return new Distributor<>(capacity);
  }

  /** A handle for each worker thread to offer new items, and drain the remaining ones when done. */
  @NotThreadSafe
  public interface ForThread<T> extends Consumer<T>, AutoCloseable {

    void finish();

    void drain();

    @Override
    void close();
  }

  public ForThread<T> forThread(Consumer<T> consumer) {
    working.incrementAndGet();
    return new ForThread<>() {
      boolean finished = false;

      @Override
      public void accept(T t) {
        if (finished) {
          throw new IllegalStateException("Finished");
        }
        if (done.get() == 0 || !pending.offer(t)) {
          consumer.accept(t);
        }
      }

      @Override
      public void finish() {
        if (!finished) {
          done.incrementAndGet();
          working.decrementAndGet();
          finished = true;
        }
      }

      @Override
      public void drain() {
        T item;
        while ((item = pending.poll()) != null || working.get() > 0) {
          if (item == null) {
            try {
              item = pending.poll(100, TimeUnit.MILLISECONDS);
              if (item == null && working.get() <= 0) {
                break;
              }
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
              break;
            }
          }
          if (item != null) {
            consumer.accept(item);
          }
        }
      }

      @Override
      public void close() {
        finish();
        drain();
      }
    };
  }
}
