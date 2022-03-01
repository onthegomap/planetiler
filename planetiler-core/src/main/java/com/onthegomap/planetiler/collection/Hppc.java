package com.onthegomap.planetiler.collection;


import com.carrotsearch.hppc.HashOrderMixing;
import com.carrotsearch.hppc.HashOrderMixingStrategy;
import com.carrotsearch.hppc.IntObjectHashMap;
import com.carrotsearch.hppc.LongByteHashMap;
import com.carrotsearch.hppc.LongByteMap;
import com.carrotsearch.hppc.LongIntHashMap;
import com.carrotsearch.hppc.LongLongHashMap;
import com.carrotsearch.hppc.LongObjectHashMap;
import com.carrotsearch.hppc.ObjectIntHashMap;

public class Hppc {

  private static final HashOrderMixingStrategy DETERMINISTIC = HashOrderMixing.constant(123321123321123312L);

  public static <T> IntObjectHashMap<T> newIntObjectHashMap() {
    return new IntObjectHashMap<>(10, 0.75, DETERMINISTIC);
  }

  public static <T> ObjectIntHashMap<T> newObjectIntHashMap() {
    return new ObjectIntHashMap<>(10, 0.75, DETERMINISTIC);
  }

  public static LongLongHashMap newLongLongHashMap() {
    return new LongLongHashMap(10, 0.75, DETERMINISTIC);
  }

  public static <T> LongObjectHashMap<T> newLongObjectHashMap() {
    return new LongObjectHashMap<>(10, 0.75, DETERMINISTIC);
  }

  public static <T> LongObjectHashMap<T> newLongObjectHashMap(int size) {
    return new LongObjectHashMap<>(size, 0.75, DETERMINISTIC);
  }

  public static LongIntHashMap newLongIntHashMap() {
    return new LongIntHashMap(10, 0.75, DETERMINISTIC);
  }

  public static LongByteMap newLongByteHashMap() {
    return new LongByteHashMap(10, 0.75, DETERMINISTIC);
  }
}
