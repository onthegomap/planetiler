/*
 *  Licensed to GraphHopper GmbH under one or more contributor
 *  license agreements. See the NOTICE file distributed with this work for
 *  additional information regarding copyright ownership.
 *
 *  GraphHopper GmbH licenses this file to you under the Apache License,
 *  Version 2.0 (the "License"); you may not use this file except in
 *  compliance with the License. You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.onthegomap.planetiler.collection;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.carrotsearch.hppc.IntArrayList;
import com.carrotsearch.hppc.IntHashSet;
import com.carrotsearch.hppc.IntSet;
import java.util.PriorityQueue;
import java.util.Random;
import org.junit.jupiter.api.Test;


/**
 * Ported from <a href=
 * "https://github.com/graphhopper/graphhopper/blob/master/core/src/test/java/com/graphhopper/coll/MinHeapWithUpdateTest.java">GraphHopper</a>
 * and modified to use long instead of float values, use stable random seed for reproducibility, and to use new
 * implementations.
 */
abstract class LongMinHeapTest {

  protected LongMinHeap heap;

  abstract void create(int capacity);

  public static class BinaryTest extends LongMinHeapTest {
    @Override
    void create(int capacity) {
      heap = LongMinHeap.newBinaryArrayHeap(capacity);
    }
  }

  public static class QuaternaryTest extends LongMinHeapTest {
    @Override
    void create(int capacity) {
      heap = LongMinHeap.newQuaternaryArrayHeap(capacity);
    }
  }


  @Test
  void outOfRange() {
    create(4);
    assertThrows(IllegalArgumentException.class, () -> heap.push(4, 12L));
    assertThrows(IllegalArgumentException.class, () -> heap.push(-1, 12L));
  }

  @Test
  void tooManyElements() {
    create(3);
    heap.push(1, 1L);
    heap.push(2, 1L);
    heap.push(0, 1L);
    // pushing element 1 again is not allowed (but this is not checked explicitly). however pushing more elements
    // than 3 is already an error
    assertThrows(IllegalStateException.class, () -> heap.push(1, 1L));
    assertThrows(IllegalStateException.class, () -> heap.push(2, 61L));
  }

  @Test
  void duplicateElements() {
    create(5);
    heap.push(1, 2L);
    heap.push(0, 4L);
    heap.push(2, 1L);
    assertEquals(2, heap.poll());
    // pushing 2 again is ok because it was polled before
    heap.push(2, 6L);
    // but now its not ok to push it again
    assertThrows(IllegalStateException.class, () -> heap.push(2, 4L));
  }

  @Test
  void testContains() {
    create(4);
    heap.push(1, 1L);
    heap.push(2, 7L);
    heap.push(0, 5L);
    assertFalse(heap.contains(3));
    assertTrue(heap.contains(1));
    assertEquals(1, heap.poll());
    assertFalse(heap.contains(1));
  }

  @Test
  void containsAfterClear() {
    create(4);
    heap.push(1, 1L);
    heap.push(2, 1L);
    assertEquals(2, heap.size());
    heap.clear();
    assertFalse(heap.contains(0));
    assertFalse(heap.contains(1));
    assertFalse(heap.contains(2));
  }


  @Test
  void testSize() {
    create(10);
    assertEquals(0, heap.size());
    assertTrue(heap.isEmpty());
    heap.push(9, 36L);
    heap.push(5, 23L);
    heap.push(3, 23L);
    assertEquals(3, heap.size());
    assertFalse(heap.isEmpty());
  }

  @Test
  void testClear() {
    create(5);
    assertTrue(heap.isEmpty());
    heap.push(3, 12L);
    heap.push(4, 3L);
    assertEquals(2, heap.size());
    heap.clear();
    assertTrue(heap.isEmpty());

    heap.push(4, 63L);
    heap.push(1, 21L);
    assertEquals(2, heap.size());
    assertEquals(1, heap.peekId());
    assertEquals(21L, heap.peekValue());
    assertEquals(1, heap.poll());
    assertEquals(4, heap.poll());
    assertTrue(heap.isEmpty());
  }

  @Test
  void testPush() {
    create(5);

    heap.push(4, 63L);
    heap.push(1, 21L);
    assertEquals(2, heap.size());
    assertEquals(1, heap.peekId());
    assertEquals(21L, heap.peekValue());
    assertEquals(1, heap.poll());
    assertEquals(4, heap.poll());
    assertTrue(heap.isEmpty());
  }

  @Test
  void testPeek() {
    create(5);
    heap.push(4, -16L);
    heap.push(2, 13L);
    heap.push(1, -51L);
    heap.push(3, 4L);
    assertEquals(1, heap.peekId());
    assertEquals(-51L, heap.peekValue());
  }

  @Test
  void pushAndPoll() {
    create(10);
    heap.push(9, 36L);
    heap.push(5, 23L);
    heap.push(3, 23L);
    assertEquals(3, heap.size());
    heap.poll();
    assertEquals(2, heap.size());
    heap.poll();
    heap.poll();
    assertTrue(heap.isEmpty());
  }

  @Test
  void pollSorted() {
    create(10);
    heap.push(9, 36L);
    heap.push(5, 21L);
    heap.push(3, 23L);
    heap.push(8, 57L);
    heap.push(7, 22L);
    IntArrayList polled = new IntArrayList();
    while (!heap.isEmpty()) {
      polled.add(heap.poll());
    }
    assertEquals(IntArrayList.from(5, 7, 3, 9, 8), polled);
  }

  @Test
  void poll() {
    create(10);
    heap.push(9, 36L);
    heap.push(5, 21L);
    heap.push(3, 23L);
    heap.push(8, 57L);
    assertEquals(5, heap.poll());
    assertEquals(3, heap.poll());
    assertEquals(9, heap.poll());
    assertEquals(8, heap.poll());
  }

  @Test
  void poll100Ascending() {
    create(100);
    for (int i = 1; i < 100; i++) {
      heap.push(i, i);
    }
    for (int i = 1; i < 100; i++) {
      assertEquals(i, heap.poll());
    }
  }

  @Test
  void poll100Descending() {
    create(100);
    for (int i = 99; i >= 1; i--) {
      heap.push(i, i);
    }
    for (int i = 1; i < 100; i++) {
      assertEquals(i, heap.poll());
    }
  }

  @Test
  void update() {
    create(10);
    heap.push(9, 36L);
    heap.push(5, 21L);
    heap.push(3, 23L);
    heap.update(3, 1L);
    assertEquals(3, heap.peekId());
    heap.update(3, 100L);
    assertEquals(5, heap.peekId());
    heap.update(9, -13L);
    assertEquals(9, heap.peekId());
    assertEquals(-13L, heap.peekValue());
    IntArrayList polled = new IntArrayList();
    while (!heap.isEmpty()) {
      polled.add(heap.poll());
    }
    assertEquals(IntArrayList.from(9, 5, 3), polled);
  }

  @Test
  void randomPushsThenPolls() {
    Random rnd = new Random(0);
    int size = 1 + rnd.nextInt(100);
    PriorityQueue<Entry> pq = new PriorityQueue<>(size);
    create(size);
    IntSet set = new IntHashSet();
    while (pq.size() < size) {
      int id = rnd.nextInt(size);
      if (!set.add(id))
        continue;
      long val = (long) (Long.MAX_VALUE * rnd.nextFloat());
      pq.add(new Entry(id, val));
      heap.push(id, val);
    }
    while (!pq.isEmpty()) {
      Entry entry = pq.poll();
      assertEquals(entry.val, heap.peekValue());
      assertEquals(entry.id, heap.poll());
      assertEquals(pq.size(), heap.size());
    }
  }

  @Test
  void randomPushsAndPolls() {
    Random rnd = new Random(0);
    int size = 1 + rnd.nextInt(100);
    PriorityQueue<Entry> pq = new PriorityQueue<>(size);
    create(size);
    IntSet set = new IntHashSet();
    int pushCount = 0;
    for (int i = 0; i < 1000; i++) {
      boolean push = pq.isEmpty() || (rnd.nextBoolean());
      if (push) {
        int id = rnd.nextInt(size);
        if (!set.add(id))
          continue;
        long val = (long) (Long.MAX_VALUE * rnd.nextFloat());
        pq.add(new Entry(id, val));
        heap.push(id, val);
        pushCount++;
      } else {
        Entry entry = pq.poll();
        assert entry != null;
        assertEquals(entry.val, heap.peekValue());
        assertEquals(entry.id, heap.poll());
        assertEquals(pq.size(), heap.size());
        set.removeAll(entry.id);
      }
    }
    assertTrue(pushCount > 0);
  }

  static class Entry implements Comparable<Entry> {
    int id;
    long val;

    public Entry(int id, long val) {
      this.id = id;
      this.val = val;
    }

    @Override
    public int compareTo(Entry o) {
      return Long.compare(val, o.val);
    }
  }
}
