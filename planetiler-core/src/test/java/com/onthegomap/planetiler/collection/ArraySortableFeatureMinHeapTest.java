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
class ArraySortableFeatureMinHeapTest {

  protected SortableFeatureMinHeap heap;

  void create(int capacity) {
    heap = SortableFeatureMinHeap.newArrayHeap(capacity);
  }

  private SortableFeature newEntry(long i) {
    return new SortableFeature(i, new byte[]{(byte) i, (byte) (1 + i)});
  }

  private SortableFeature newEntry(long i, byte[] v) {
    return new SortableFeature(i, v);
  }

  @Test
  void outOfRange() {
    create(4);
    assertThrows(IllegalArgumentException.class, () -> heap.push(4, newEntry(12L)));
    assertThrows(IllegalArgumentException.class, () -> heap.push(-1, newEntry(12L)));
  }

  @Test
  void tooManyElements() {
    create(3);
    heap.push(1, newEntry(1L));
    heap.push(2, newEntry(1L));
    heap.push(0, newEntry(1L));
    // pushing element 1 again is not allowed (but this is not checked explicitly). however pushing more elements
    // than 3 is already an error
    assertThrows(IllegalStateException.class, () -> heap.push(1, newEntry(1L)));
    assertThrows(IllegalStateException.class, () -> heap.push(2, newEntry(61L)));
  }

  @Test
  void duplicateElements() {
    create(5);
    heap.push(1, newEntry(2L));
    heap.push(0, newEntry(4L));
    heap.push(2, newEntry(1L));
    assertEquals(2, heap.poll());
    // pushing 2 again is ok because it was polled before
    heap.push(2, newEntry(6L));
    // but now its not ok to push it again
    assertThrows(IllegalStateException.class, () -> heap.push(2, newEntry(4L)));
  }

  @Test
  void testContains() {
    create(4);
    heap.push(1, newEntry(1L));
    heap.push(2, newEntry(7L));
    heap.push(0, newEntry(5L));
    assertFalse(heap.contains(3));
    assertTrue(heap.contains(1));
    assertEquals(1, heap.poll());
    assertFalse(heap.contains(1));
  }

  @Test
  void containsAfterClear() {
    create(4);
    heap.push(1, newEntry(1L));
    heap.push(2, newEntry(1L));
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
    heap.push(9, newEntry(36L));
    heap.push(5, newEntry(23L));
    heap.push(3, newEntry(23L));
    assertEquals(3, heap.size());
    assertFalse(heap.isEmpty());
  }

  @Test
  void testClear() {
    create(5);
    assertTrue(heap.isEmpty());
    heap.push(3, newEntry(12L));
    heap.push(4, newEntry(3L));
    assertEquals(2, heap.size());
    heap.clear();
    assertTrue(heap.isEmpty());

    heap.push(4, newEntry(63L));
    heap.push(1, newEntry(21L));
    assertEquals(2, heap.size());
    assertEquals(1, heap.peekId());
    assertEquals(newEntry(21L), heap.peekValue());
    assertEquals(1, heap.poll());
    assertEquals(4, heap.poll());
    assertTrue(heap.isEmpty());
  }

  @Test
  void testPush() {
    create(5);

    heap.push(4, newEntry(63L));
    heap.push(1, newEntry(21L));
    assertEquals(2, heap.size());
    assertEquals(1, heap.peekId());
    assertEquals(newEntry(21L), heap.peekValue());
    assertEquals(1, heap.poll());
    assertEquals(4, heap.poll());
    assertTrue(heap.isEmpty());
  }

  @Test
  void testPeek() {
    create(5);
    heap.push(4, newEntry(-16L));
    heap.push(2, newEntry(13L));
    heap.push(1, newEntry(-51L));
    heap.push(3, newEntry(4L));
    assertEquals(1, heap.peekId());
    assertEquals(newEntry(-51L), heap.peekValue());
  }

  @Test
  void pushAndPoll() {
    create(10);
    heap.push(9, newEntry(36L));
    heap.push(5, newEntry(23L));
    heap.push(3, newEntry(23L));
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
    heap.push(9, newEntry(36L));
    heap.push(5, newEntry(21L));
    heap.push(3, newEntry(23L));
    heap.push(8, newEntry(57L));
    heap.push(7, newEntry(22L));
    IntArrayList polled = new IntArrayList();
    while (!heap.isEmpty()) {
      polled.add(heap.poll());
    }
    assertEquals(IntArrayList.from(5, 7, 3, 9, 8), polled);
  }

  @Test
  void poll() {
    create(10);
    assertTrue(heap.isEmpty());
    assertEquals(0, heap.size());

    heap.push(9, newEntry(36L));
    assertFalse(heap.isEmpty());
    assertEquals(1, heap.size());

    heap.push(5, newEntry(21L));
    assertFalse(heap.isEmpty());
    assertEquals(2, heap.size());

    heap.push(3, newEntry(23L));
    assertFalse(heap.isEmpty());
    assertEquals(3, heap.size());

    heap.push(8, newEntry(57L));
    assertFalse(heap.isEmpty());
    assertEquals(4, heap.size());

    assertEquals(5, heap.poll());
    assertFalse(heap.isEmpty());
    assertEquals(3, heap.size());

    assertEquals(3, heap.poll());
    assertFalse(heap.isEmpty());
    assertEquals(2, heap.size());

    assertEquals(9, heap.poll());
    assertFalse(heap.isEmpty());
    assertEquals(1, heap.size());

    assertEquals(8, heap.poll());
    assertTrue(heap.isEmpty());
    assertEquals(0, heap.size());
  }

  @Test
  void clear() {
    create(10);
    heap.push(9, newEntry(36L));
    heap.push(5, newEntry(21L));
    heap.push(3, newEntry(23L));
    heap.clear();
    assertTrue(heap.isEmpty());
    assertEquals(0, heap.size());
  }

  @Test
  void poll100Ascending() {
    create(100);
    for (int i = 1; i < 100; i++) {
      heap.push(i, newEntry(i));
    }
    for (int i = 1; i < 100; i++) {
      assertEquals(i, heap.poll());
    }
  }

  @Test
  void poll100Descending() {
    create(100);
    for (int i = 99; i >= 1; i--) {
      heap.push(i, newEntry(i));
    }
    for (int i = 1; i < 100; i++) {
      assertEquals(i, heap.poll());
    }
  }

  @Test
  void update() {
    create(10);
    heap.push(9, newEntry(36L));
    heap.push(5, newEntry(21L));
    heap.push(3, newEntry(23L));
    heap.update(3, newEntry(1L));
    assertEquals(3, heap.peekId());
    heap.update(3, newEntry(100L));
    assertEquals(5, heap.peekId());
    heap.update(9, newEntry(-13L));
    assertEquals(9, heap.peekId());
    assertEquals(newEntry(-13L), heap.peekValue());
    IntArrayList polled = new IntArrayList();
    while (!heap.isEmpty()) {
      polled.add(heap.poll());
    }
    assertEquals(IntArrayList.from(9, 5, 3), polled);
  }

  @Test
  void updateWithEqualKeys() {
    create(10);
    heap.push(9, newEntry(36L));
    heap.push(5, newEntry(21L, new byte[]{(byte) 1, (byte) 2}));
    heap.push(3, newEntry(23L));
    heap.update(3, newEntry(1L));
    assertEquals(3, heap.peekId());
    heap.update(3, newEntry(100L));
    assertEquals(5, heap.peekId());
    // until here same as update() test, now some "key collisions"

    // prepare for hitting entry id=5 with sortable feature which has same ID but different value
    heap.update(9, newEntry(21L, new byte[]{(byte) 100, (byte) 200}));
    assertEquals(5, heap.peekId());

    // hit "percolate up", NOT replacing item id=5
    heap.update(9, newEntry(21L, new byte[]{(byte) 10, (byte) 20}));
    assertEquals(5, heap.peekId());

    // hit "percolate down"
    heap.update(9, newEntry(21L, new byte[]{(byte) 20, (byte) 30}));
    assertEquals(5, heap.peekId());

    // hit "percolate up", still NOT replacing item id=5
    heap.update(9, newEntry(21L, new byte[]{(byte) 5, (byte) 10}));
    assertEquals(5, heap.peekId());

    // hit "percolate up" one last time, now replacing item id=5
    SortableFeature SF = newEntry(21L, new byte[]{(byte) 0, (byte) 0});
    heap.update(9, SF);
    assertEquals(9, heap.peekId());
    assertEquals(SF, heap.peekValue());

    // and from now on again same as update() test
    heap.update(9, newEntry(-13L));
    assertEquals(9, heap.peekId());
    assertEquals(newEntry(-13L), heap.peekValue());
    IntArrayList polled = new IntArrayList();
    while (!heap.isEmpty()) {
      polled.add(heap.poll());
    }
    assertEquals(IntArrayList.from(9, 5, 3), polled);
  }

  @Test
  void updateHead() {
    create(10);
    heap.push(1, newEntry(1));
    heap.push(2, newEntry(2));
    heap.push(3, newEntry(3));
    heap.push(4, newEntry(4));
    heap.push(5, newEntry(5));
    heap.updateHead(newEntry(6));
    heap.updateHead(newEntry(7));
    heap.updateHead(newEntry(8));

    IntArrayList polled = new IntArrayList();
    while (!heap.isEmpty()) {
      polled.add(heap.poll());
    }
    assertEquals(IntArrayList.from(4, 5, 1, 2, 3), polled);
  }

  @Test
  void randomPushsThenPolls() {
    Random rnd = new Random(0);
    int size = 1 + rnd.nextInt(100);
    PriorityQueue<SortableFeature> pq = new PriorityQueue<>(size);
    create(size);
    IntSet set = new IntHashSet();
    while (pq.size() < size) {
      int id = rnd.nextInt(size);
      if (!set.add(id))
        continue;
      long key = (long) (Long.MAX_VALUE * rnd.nextFloat());
      byte[] value = new byte[]{(byte) id, (byte) (id + 1)};
      SortableFeature sf = new SortableFeature(key, value);
      pq.add(sf);
      heap.push(id, sf);
    }
    while (!pq.isEmpty()) {
      SortableFeature entry = pq.poll();
      assertEquals(entry, heap.peekValue());
      assertEquals(entry.value()[0], (byte) heap.poll());
      assertEquals(pq.size(), heap.size());
    }
  }

  @Test
  void randomPushsAndPolls() {
    Random rnd = new Random(0);
    int size = 1 + rnd.nextInt(100);
    PriorityQueue<SortableFeature> pq = new PriorityQueue<>(size);
    create(size);
    IntSet set = new IntHashSet();
    int pushCount = 0;
    for (int i = 0; i < 1000; i++) {
      boolean push = pq.isEmpty() || (rnd.nextBoolean());
      if (push) {
        int id = rnd.nextInt(size);
        if (!set.add(id))
          continue;
        long key = (long) (Long.MAX_VALUE * rnd.nextFloat());
        byte[] value = new byte[]{(byte) id, (byte) (id + 1)};
        SortableFeature sf = new SortableFeature(key, value);
        heap.push(id, sf);
        pushCount++;
      } else {
        SortableFeature entry = pq.poll();
        assert entry != null;
        assertEquals(entry, heap.peekValue());
        int id = heap.poll();
        assertEquals(entry.value()[0], (byte) id);
        assertEquals(pq.size(), heap.size());
        set.removeAll(id);
      }
    }
    assertTrue(pushCount > 0);
  }
}
