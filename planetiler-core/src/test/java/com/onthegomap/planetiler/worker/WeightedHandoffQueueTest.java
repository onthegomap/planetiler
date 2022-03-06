package com.onthegomap.planetiler.worker;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

public class WeightedHandoffQueueTest {

  @Test
  @Timeout(10)
  public void testEmpty() {
    WeightedHandoffQueue<String> q = new WeightedHandoffQueue<>(1, 1);
    q.close();
    assertNull(q.get());
  }

  @Test
  @Timeout(10)
  public void testOneItem() {
    WeightedHandoffQueue<String> q = new WeightedHandoffQueue<>(1, 1);
    q.accept("a", 1);
    assertEquals("a", q.get());
    q.close();
    assertNull(q.get());
  }

  @Test
  @Timeout(10)
  public void testOneItemCloseFirst() {
    WeightedHandoffQueue<String> q = new WeightedHandoffQueue<>(2, 1);
    q.accept("a", 1);
    q.close();
    assertEquals("a", q.get());
    assertNull(q.get());
  }

  @Test
  @Timeout(10)
  public void testMoreItemsThanBatchSize() {
    WeightedHandoffQueue<String> q = new WeightedHandoffQueue<>(3, 2);
    q.accept("a", 1);
    q.accept("b", 1);
    q.accept("c", 1);
    q.close();
    assertEquals("a", q.get());
    assertEquals("b", q.get());
    assertEquals("c", q.get());
    assertNull(q.get());
  }

  @Test
  @Timeout(10)
  public void testManyItems() {
    WeightedHandoffQueue<Integer> q = new WeightedHandoffQueue<>(100, 100);
    for (int i = 0; i < 950; i++) {
      q.accept(i, 1);
    }
    q.close();
    for (int i = 0; i < 950; i++) {
      assertEquals((Integer) i, q.get());
    }
    assertNull(q.get());
  }
}
