package com.onthegomap.planetiler.worker;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import com.onthegomap.planetiler.stats.Stats;
import java.util.Set;
import java.util.TreeSet;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

class WorkQueueTest {

  @Test
  @Timeout(10)
  void testEmpty() {
    WorkQueue<String> q = newQueue(1);
    q.close();
    assertNull(q.get());
  }

  @Test
  @Timeout(10)
  void testOneItem() {
    WorkQueue<String> q = newQueue(1);
    q.accept("a");
    q.close();
    assertEquals("a", q.get());
    assertNull(q.get());
  }

  @Test
  @Timeout(10)
  void testMoreItemsThanBatchSize() {
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
  void testManyItems() {
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
  void testTwoWriters() {
    WorkQueue<Integer> q = newQueue(2);
    new Worker("worker", stats, 2, q::accept).await();
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
  void testTwoWritersManyElements() {
    WorkQueue<Integer> q = newQueue(2);
    new Worker("worker", stats, 2, i -> {
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

  @Test
  @Timeout(10)
  void testHandsOffBatchOnceMaxWeightReached() {
    WorkQueue<Integer> q = new WorkQueue<>("queue", 1000, 16, i -> i, 10, stats);
    q.accept(3);
    q.accept(4);
    q.accept(5);
    // get() on an open queue only returns once the writer handed off a batch
    assertEquals(3, q.get());
    assertEquals(4, q.get());
    assertEquals(5, q.get());
    q.accept(1);
    q.close();
    assertEquals(1, q.get());
    assertNull(q.get());
  }

  @Test
  @Timeout(10)
  void testItemHeavierThanMaxWeightHandedOffAlone() {
    WorkQueue<Integer> q = new WorkQueue<>("queue", 1000, 16, i -> i, 10, stats);
    q.accept(100);
    assertEquals(100, q.get());
    q.accept(200);
    assertEquals(200, q.get());
    q.close();
    assertNull(q.get());
  }

  @Test
  @Timeout(10)
  void testWeightedQueueStillHandsOffFullBatches() {
    WorkQueue<Integer> q = new WorkQueue<>("queue", 1000, 2, i -> 0, 10, stats);
    q.accept(1);
    q.accept(2);
    assertEquals(1, q.get());
    assertEquals(2, q.get());
    q.close();
    assertNull(q.get());
  }

  private <T> WorkQueue<T> newQueue(int maxBatch) {
    return new WorkQueue<>("queue", 1000, maxBatch, stats);
  }

  private static final Stats stats = Stats.inMemory();
}
