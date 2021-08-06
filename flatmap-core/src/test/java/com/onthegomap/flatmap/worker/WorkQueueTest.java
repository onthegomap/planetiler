package com.onthegomap.flatmap.worker;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import com.onthegomap.flatmap.stats.Stats;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

public class WorkQueueTest {

  @Test
  @Timeout(10)
  public void testEmpty() {
    WorkQueue<String> q = newQueue(1);
    q.close();
    assertNull(q.get());
  }

  @Test
  @Timeout(10)
  public void testOneItem() {
    WorkQueue<String> q = newQueue(1);
    q.accept("a");
    q.close();
    assertEquals("a", q.get());
    assertNull(q.get());
  }

  @Test
  @Timeout(10)
  public void testMoreItemsThanBatchSize() {
    WorkQueue<String> q = newQueue(2);
    q.accept("a");
    q.accept("b");
    q.accept("c");
    q.close();
    assertEquals("a", q.get());
    assertEquals("b", q.get());
    assertEquals("c", q.get());
    assertNull(q.get());
  }

  @Test
  @Timeout(10)
  public void testManyItems() {
    WorkQueue<Integer> q = newQueue(100);
    for (int i = 0; i < 950; i++) {
      q.accept(i);
    }
    q.close();
    for (int i = 0; i < 950; i++) {
      assertEquals((Integer) i, q.get());
    }
    assertNull(q.get());
  }

  @Test
  @Timeout(10)
  public void testTwoWriters() {
    WorkQueue<Integer> q = newQueue(2);
    AtomicInteger ni = new AtomicInteger(0);
    new Worker("worker", stats, 2, () -> {
      int i = ni.getAndIncrement();
      q.accept(i);
    }).await();
    q.close();
    assertEquals(2, q.getPending());
    Set<Integer> found = new TreeSet<>();
    for (int i = 0; i < 2; i++) {
      found.add(q.get());
    }
    assertNull(q.get());
    assertEquals(Set.of(0, 1), found);
    assertEquals(0, q.getPending());
  }

  @Test
  @Timeout(10)
  public void testTwoWritersManyElements() {
    WorkQueue<Integer> q = newQueue(2);
    AtomicInteger ni = new AtomicInteger(0);
    new Worker("worker", stats, 2, () -> {
      int i = ni.getAndIncrement();
      q.accept(i * 3);
      q.accept(i * 3 + 1);
      q.accept(i * 3 + 2);
    }).await();
    q.close();
    assertEquals(6, q.getPending());
    Set<Integer> found = new TreeSet<>();
    for (int i = 0; i < 6; i++) {
      found.add(q.get());
    }
    assertNull(q.get());
    assertEquals(Set.of(0, 1, 2, 3, 4, 5), found);
    assertEquals(0, q.getPending());
  }

  private <T> WorkQueue<T> newQueue(int maxBatch) {
    return new WorkQueue<>("queue", 1000, maxBatch, stats);
  }

  private static final Stats stats = Stats.inMemory();
}
