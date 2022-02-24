package com.onthegomap.planetiler.collection;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import org.junit.jupiter.api.Test;

public class IterableOnceTest {

  @Test
  public void testIterableOnceEmpty() {
    IterableOnce<Integer> empty = () -> null;
    var iter = empty.iterator();
    assertFalse(iter.hasNext());
    assertNull(iter.next());
    assertFalse(iter.hasNext());
    assertNull(iter.next());
  }

  @Test
  public void testSingleItem() {
    Queue<Integer> queue = new LinkedList<>(List.of(1));
    IterableOnce<Integer> iterable = queue::poll;
    var iter = iterable.iterator();
    assertTrue(iter.hasNext());
    assertEquals(1, iter.next());
    assertFalse(iter.hasNext());
    assertNull(iter.next());
  }

  @Test
  public void testMultipleItems() {
    Queue<Integer> queue = new LinkedList<>(List.of(1, 2));
    IterableOnce<Integer> iterable = queue::poll;
    var iter = iterable.iterator();
    assertTrue(iter.hasNext());
    assertEquals(1, iter.next());
    assertTrue(iter.hasNext());
    assertEquals(2, iter.next());
    assertFalse(iter.hasNext());
    assertNull(iter.next());
  }

  @Test
  public void testMultipleIterators() {
    Queue<Integer> queue = new LinkedList<>(List.of(1, 2));
    IterableOnce<Integer> iterable = queue::poll;
    var iter1 = iterable.iterator();
    var iter2 = iterable.iterator();
    assertTrue(iter1.hasNext());
    assertTrue(iter2.hasNext());
    assertEquals(1, iter1.next());
    assertFalse(iter1.hasNext());
    assertTrue(iter2.hasNext());
    assertEquals(2, iter2.next());
    assertFalse(iter1.hasNext());
    assertFalse(iter2.hasNext());
  }

  @Test
  public void testForeach() {
    Queue<Integer> queue = new LinkedList<>(List.of(1, 2, 3, 4));
    IterableOnce<Integer> iterable = queue::poll;
    Set<Integer> result = new HashSet<>();
    for (var item : iterable) {
      result.add(item);
    }
    assertEquals(Set.of(1, 2, 3, 4), result);
  }

  @Test
  public void testForeachWithSupplierAccess() {
    Queue<Integer> queue = new LinkedList<>(List.of(1, 2, 3, 4));
    IterableOnce<Integer> iterable = queue::poll;
    List<Integer> result = new ArrayList<>();
    int iters = 0;
    for (var item : iterable) {
      result.add(item);
      Integer item2 = iterable.get();
      if (item2 != null) {
        result.add(item2);
      }
      iters++;
    }
    assertEquals(List.of(1, 2, 3, 4), result.stream().sorted().toList());
    assertEquals(3, iters);
  }
}
