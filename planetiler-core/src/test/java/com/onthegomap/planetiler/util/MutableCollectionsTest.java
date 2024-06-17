package com.onthegomap.planetiler.util;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.junit.jupiter.api.Test;

class MutableCollectionsTest {
  @Test
  void testListOf() {
    var mutable = MutableCollections.makeMutable(List.of(1, 2, 3));
    mutable.add(4);
    assertEquals(List.of(1, 2, 3, 4), mutable);
  }

  @Test
  void testListOf0() {
    var mutable = MutableCollections.makeMutable(List.<Integer>of());
    mutable.add(1);
    mutable.add(2);
    mutable.add(3);
    mutable.add(4);
    assertEquals(List.of(1, 2, 3, 4), mutable);
  }

  @Test
  void testArrayList() {
    var mutable = MutableCollections.makeMutable(new ArrayList<>(List.of(1, 2, 3)));
    mutable.add(4);
    assertEquals(List.of(1, 2, 3, 4), mutable);
  }

  @Test
  void testLinkedList() {
    var mutable = MutableCollections.makeMutable(new LinkedList<>(List.of(1, 2, 3)));
    mutable.add(4);
    assertEquals(List.of(1, 2, 3, 4), mutable);
  }

  @Test
  void testUnmodifiableCollection() {
    var mutable = MutableCollections.makeMutable(Collections.unmodifiableList(new ArrayList<>(List.of(1, 2, 3))));
    mutable.add(4);
    assertEquals(List.of(1, 2, 3, 4), mutable);
  }

  @Test
  void testGuavaList() {
    var mutable = MutableCollections.makeMutable(ImmutableList.builder().add(1, 2, 3).build());
    mutable.add(4);
    assertEquals(List.of(1, 2, 3, 4), mutable);
  }

  @Test
  void testMapOs() {
    var mutable = MutableCollections.makeMutableMap(Map.of(1, 2, 3, 4));
    mutable.put(5, 6);
    assertEquals(Map.of(1, 2, 3, 4, 5, 6), mutable);
  }

  @Test
  void testHashMap() {
    var mutable = MutableCollections.makeMutableMap(new HashMap<>(Map.of(1, 2, 3, 4)));
    mutable.put(5, 6);
    assertEquals(Map.of(1, 2, 3, 4, 5, 6), mutable);
  }

  @Test
  void testTreeMap() {
    var mutable = MutableCollections.makeMutableMap(new TreeMap<>(Map.of(1, 2, 3, 4)));
    mutable.put(5, 6);
    assertEquals(Map.of(1, 2, 3, 4, 5, 6), mutable);
  }

  @Test
  void testUnmodifiableMap() {
    var mutable = MutableCollections.makeMutableMap(Collections.unmodifiableMap(new TreeMap<>(Map.of(1, 2, 3, 4))));
    mutable.put(5, 6);
    assertEquals(Map.of(1, 2, 3, 4, 5, 6), mutable);
  }

  @Test
  void testGuavaMap() {
    var mutable = MutableCollections.makeMutableMap(ImmutableMap.builder().put(1, 2).put(3, 4).build());
    mutable.put(5, 6);
    assertEquals(Map.of(1, 2, 3, 4, 5, 6), mutable);
  }

  @Test
  void testMultimap() {
    var mutable = MutableCollections.makeMutableMultimap(Map.of(1, List.of(2, 3), 4, List.of(5, 6)));
    var map = mutable.get(1);
    map.add(3);
    mutable.put(7, map);
    assertEquals(Map.of(1, List.of(2, 3, 3), 4, List.of(5, 6), 7, List.of(2, 3, 3)), mutable);
  }
}
