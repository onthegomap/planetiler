package com.onthegomap.planetiler.util;

import java.util.AbstractSequentialList;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.SequencedMap;
import java.util.TreeMap;

/** Utilities for converting immutable collections to mutable ones. */
public class MutableCollections {
  private MutableCollections() {}

  /** Return a mutable copy of {@code list} or the original list if it is already mutable. */
  public static <T> List<T> makeMutable(List<T> list) {
    return switch (list) {
      case ArrayList<T> l -> l;
      case LinkedList<T> l -> l;
      case AbstractSequentialList<T> l -> new LinkedList<>(l);
      case null -> list;
      default -> new ArrayList<>(list);
    };
  }

  /**
   * Return a mutable copy of {@code map} with mutable list values or the original collections if they are already
   * mutable.
   */
  public static <K, V> Map<K, List<V>> makeMutableMultimap(Map<K, List<V>> map) {
    var mutableMap = makeMutableMap(map);
    if (mutableMap != null) {
      for (var entry : map.entrySet()) {
        var key = entry.getKey();
        var value = entry.getValue();
        var mutableList = makeMutable(value);
        if (mutableList != value) {
          mutableMap.put(key, mutableList);
        }
      }
    }
    return mutableMap;
  }

  /** Return a mutable copy of {@code map} or the original list if it is already mutable. */
  public static <K, V> Map<K, V> makeMutableMap(Map<K, V> map) {
    return switch (map) {
      case HashMap<K, V> m -> m;
      case TreeMap<K, V> m -> m;
      case NavigableMap<K, V> m -> new TreeMap<>(m);
      case SequencedMap<K, V> m -> new LinkedHashMap<>(m);
      case null -> map;
      default -> new HashMap<>(map);
    };
  }
}
