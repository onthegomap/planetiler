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

import java.util.function.IntBinaryOperator;

/**
 * API for min-heaps that keeps track of {@code int} keys in a range from {@code [0, size)} ordered by {@code double}
 * values.
 * <p>
 * Ported from <a href=
 * "https://github.com/graphhopper/graphhopper/blob/master/core/src/main/java/com/graphhopper/coll/MinHeapWithUpdate.java">GraphHopper</a>
 * and modified to extract a common interface for subclass implementations.
 */
public interface DoubleMinHeap {
  /**
   * Returns a new min-heap where each element has 4 children backed by elements in an array.
   * <p>
   * This is slightly faster than a traditional binary min heap due to a shallower, more cache-friendly memory layout.
   */
  static DoubleMinHeap newArrayHeap(int elements, IntBinaryOperator tieBreaker) {
    return new ArrayDoubleMinHeap(elements, tieBreaker);
  }

  int size();

  boolean isEmpty();

  /**
   * Adds an element to the heap, the given id must not exceed the size specified in the constructor. Its illegal to
   * push the same id twice (unless it was polled/removed before). To update the value of an id contained in the heap
   * use the {@link #update} method.
   */
  void push(int id, double value);

  /**
   * @return true if the heap contains an element with the given id
   */
  boolean contains(int id);

  /**
   * Updates the element with the given id. The complexity of this method is O(log(N)), just like push/poll. Its illegal
   * to update elements that are not contained in the heap. Use {@link #contains} to check the existence of an id.
   */
  void update(int id, double value);

  /**
   * Updates the weight of the head element in the heap, pushing it down and bubbling up the new min element if
   * necessary.
   */
  void updateHead(double value);

  /**
   * @return the id of the next element to be polled, i.e. the same as calling poll() without removing the element
   */
  int peekId();

  double peekValue();

  /**
   * Extracts the element with minimum value from the heap
   */
  int poll();

  void clear();
}
