package com.onthegomap.planetiler.util;

import java.util.HashMap;
import java.util.Map;

public class MapUtil {
  private MapUtil() {}

  /**
   * Returns a new map with the union of entries from {@code a} and {@code b} where conflicts take the value from
   * {@code b}.
   */
  public static <K, V> Map<K, V> merge(Map<K, V> a, Map<K, V> b) {
    Map<K, V> copy = new HashMap<>(a);
    copy.putAll(b);
    return copy;
  }

  /**
   * Returns a new map the entries of {@code a} added to {@code (k, v)}.
   */
  public static <K, V> Map<K, V> with(Map<K, V> a, K k, V v) {
    Map<K, V> copy = new HashMap<>(a);
    if (v == null || "".equals(v)) {
      copy.remove(k);
    } else {
      copy.put(k, v);
    }
    return copy;
  }
}
