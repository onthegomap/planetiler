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

import java.util.Arrays;
import java.util.function.IntBinaryOperator;

/**
 * A min-heap stored in an array where each element has 4 children.
 * <p>
 * This is about 5-10% faster than the standard binary min-heap for the case of merging sorted lists.
 * <p>
 * Ported from <a href=
 * "https://github.com/graphhopper/graphhopper/blob/master/core/src/main/java/com/graphhopper/coll/MinHeapWithUpdate.java">GraphHopper</a>
 * and:
 * <ul>
 * <li>modified to use {@code long} values instead of {@code float}</li>
 * <li>extracted a common interface for subclass implementations</li>
 * <li>modified so that each element has 4 children instead of 2 (improves performance by 5-10%)</li>
 * <li>performance improvements to minimize array lookups</li>
 * </ul>
 *
 * @see <a href="https://en.wikipedia.org/wiki/D-ary_heap">d-ary heap (wikipedia)</a>
 */
class ArrayLongMinHeap implements LongMinHeap {
  protected static final int NOT_PRESENT = -1;
  protected final int[] posToId;
  protected final int[] idToPos;
  protected final long[] posToValue;
  protected final int max;
  protected int size;
  private final IntBinaryOperator tieBreaker;

  /**
   * @param elements the number of elements that can be stored in this heap. Currently the heap cannot be resized or
   *                 shrunk/trimmed after initial creation. elements-1 is the maximum id that can be stored in this heap
   */
  ArrayLongMinHeap(int elements, IntBinaryOperator tieBreaker) {
    // we use an offset of one to make the arithmetic a bit simpler/more efficient, the 0th elements are not used!
    posToId = new int[elements + 1];
    idToPos = new int[elements + 1];
    Arrays.fill(idToPos, NOT_PRESENT);
    posToValue = new long[elements + 1];
    posToValue[0] = Long.MIN_VALUE;
    this.max = elements;
    this.tieBreaker = tieBreaker;
  }

  private static int firstChild(int index) {
    return (index << 2) - 2;
  }

  private static int parent(int index) {
    return (index + 2) >> 2;
  }

  @Override
  public int size() {
    return size;
  }

  @Override
  public boolean isEmpty() {
    return size == 0;
  }

  @Override
  public void push(int id, long value) {
    checkIdInRange(id);
    if (size == max) {
      throw new IllegalStateException("Cannot push anymore, the heap is already full. size: " + size);
    }
    if (contains(id)) {
      throw new IllegalStateException("Element with id: " + id +
        " was pushed already, you need to use the update method if you want to change its value");
    }
    size++;
    posToId[size] = id;
    idToPos[id] = size;
    posToValue[size] = value;
    percolateUp(size);
  }

  @Override
  public boolean contains(int id) {
    checkIdInRange(id);
    return idToPos[id] != NOT_PRESENT;
  }

  @Override
  public void update(int id, long value) {
    checkIdInRange(id);
    int pos = idToPos[id];
    if (pos < 0) {
      throw new IllegalStateException(
        "The heap does not contain: " + id + ". Use the contains method to check this before calling update");
    }
    long prev = posToValue[pos];
    posToValue[pos] = value;
    int cmp = compareIdPos(value, prev, id, pos);
    if (cmp > 0) {
      percolateDown(pos);
    } else if (cmp < 0) {
      percolateUp(pos);
    }
  }

  @Override
  public void updateHead(long value) {
    posToValue[1] = value;
    percolateDown(1);
  }

  @Override
  public int peekId() {
    return posToId[1];
  }

  @Override
  public long peekValue() {
    return posToValue[1];
  }

  @Override
  public int poll() {
    int id = peekId();
    posToId[1] = posToId[size];
    posToValue[1] = posToValue[size];
    idToPos[posToId[1]] = 1;
    idToPos[id] = NOT_PRESENT;
    size--;
    percolateDown(1);
    return id;
  }

  @Override
  public void clear() {
    for (int i = 1; i <= size; i++) {
      idToPos[posToId[i]] = NOT_PRESENT;
    }
    size = 0;
  }

  private void percolateUp(int pos) {
    assert pos != 0;
    if (pos == 1) {
      return;
    }
    final int id = posToId[pos];
    final long val = posToValue[pos];
    // the finish condition (index==0) is covered here automatically because we set vals[0]=-inf
    int parent;
    long parentValue;
    while (compareIdPos(val, parentValue = posToValue[parent = parent(pos)], id, parent) < 0) {
      posToValue[pos] = parentValue;
      idToPos[posToId[pos] = posToId[parent]] = pos;
      pos = parent;
    }
    posToId[pos] = id;
    posToValue[pos] = val;
    idToPos[posToId[pos]] = pos;
  }

  private void checkIdInRange(int id) {
    if (id < 0 || id >= max) {
      throw new IllegalArgumentException("Illegal id: " + id + ", legal range: [0, " + max + "[");
    }
  }

  private void percolateDown(int pos) {
    if (size == 0) {
      return;
    }
    assert pos > 0;
    assert pos <= size;
    final int id = posToId[pos];
    final long value = posToValue[pos];
    int child;
    while ((child = firstChild(pos)) <= size) {
      // optimization: this is a very hot code path for performance of k-way merging,
      // so manually-unroll the loop over the 4 child elements to find the minimum value
      int minChild = child;
      long minValue = posToValue[child], childValue;
      if (++child <= size) {
        if (comparePosPos(childValue = posToValue[child], minValue, child, minChild) < 0) {
          minChild = child;
          minValue = childValue;
        }
        if (++child <= size) {
          if (comparePosPos(childValue = posToValue[child], minValue, child, minChild) < 0) {
            minChild = child;
            minValue = childValue;
          }
          if (++child <= size &&
            comparePosPos(childValue = posToValue[child], minValue, child, minChild) < 0) {
            minChild = child;
            minValue = childValue;
          }
        }
      }
      if (comparePosPos(value, minValue, pos, minChild) <= 0) {
        break;
      }
      posToValue[pos] = minValue;
      idToPos[posToId[pos] = posToId[minChild]] = pos;
      pos = minChild;
    }
    posToId[pos] = id;
    posToValue[pos] = value;
    idToPos[id] = pos;
  }

  private int comparePosPos(long val1, long val2, int pos1, int pos2) {
    if (val1 < val2) {
      return -1;
    } else if (val1 == val2 && val1 != Long.MIN_VALUE) {
      return tieBreaker.applyAsInt(posToId[pos1], posToId[pos2]);
    }
    return 1;
  }

  private int compareIdPos(long val1, long val2, int id1, int pos2) {
    if (val1 < val2) {
      return -1;
    } else if (val1 == val2 && val1 != Long.MIN_VALUE) {
      return tieBreaker.applyAsInt(id1, posToId[pos2]);
    }
    return 1;
  }

}
