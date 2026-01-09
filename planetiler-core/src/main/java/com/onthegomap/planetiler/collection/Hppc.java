package com.onthegomap.planetiler.collection;

import com.carrotsearch.hppc.IntObjectHashMap;
import com.carrotsearch.hppc.LongArrayList;
import com.carrotsearch.hppc.LongByteHashMap;
import com.carrotsearch.hppc.LongByteMap;
import com.carrotsearch.hppc.LongIntHashMap;
import com.carrotsearch.hppc.LongLongHashMap;
import com.carrotsearch.hppc.LongObjectHashMap;
import com.carrotsearch.hppc.ObjectIntHashMap;
import com.carrotsearch.hppc.SortedIterationLongObjectHashMap;

/**
 * Static factory method for <a href="https://github.com/carrotsearch/hppc">High Performance Primitive Collections</a>.
 */
public class Hppc {

  public static <T> IntObjectHashMap<T> newIntObjectHashMap() {
    return new IntObjectHashMap<>(10, 0.75);
  }

  public static <T> ObjectIntHashMap<T> newObjectIntHashMap() {
    return new ObjectIntHashMap<>(10, 0.75);
  }

  public static LongLongHashMap newLongLongHashMap() {
    return new LongLongHashMap(10, 0.75);
  }

  public static <T> LongObjectHashMap<T> newLongObjectHashMap() {
    return new LongObjectHashMap<>(10, 0.75);
  }

  public static <T> LongObjectHashMap<T> newLongObjectHashMap(int size) {
    return new LongObjectHashMap<>(size, 0.75);
  }

  public static LongIntHashMap newLongIntHashMap() {
    return new LongIntHashMap(10, 0.75);
  }

  public static LongByteMap newLongByteHashMap() {
    return new LongByteHashMap(10, 0.75);
  }

  public static <T> SortedIterationLongObjectHashMap<T> sortedView(LongObjectHashMap<T> input) {
    return new SortedIterationLongObjectHashMap<>(input, Long::compare);
  }

  public static LongArrayList subList(LongArrayList nodes, int fromIndex, int toIndex) {
    int length = toIndex - fromIndex;
    LongArrayList result = new LongArrayList(length);
    result.elementsCount = length;
    System.arraycopy(nodes.buffer, fromIndex, result.buffer, 0, length);
    return result;
  }
}
