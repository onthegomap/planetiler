package com.onthegomap.planetiler.util;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.carrotsearch.hppc.LongArrayList;
import com.onthegomap.planetiler.collection.Hppc;
import org.junit.jupiter.api.Test;

class HppcTest {
  @Test
  void testSublistEmpty() {
    assertEquals(LongArrayList.from(), Hppc.subList(LongArrayList.from(), 0, 0));
  }

  @Test
  void testSublist1() {
    assertEquals(LongArrayList.from(), Hppc.subList(LongArrayList.from(1), 0, 0));
    assertEquals(LongArrayList.from(1), Hppc.subList(LongArrayList.from(1), 0, 1));
  }

  @Test
  void testSublist2() {
    LongArrayList from = LongArrayList.from(1, 2);
    assertEquals(LongArrayList.from(), Hppc.subList(from, 0, 0));
    assertEquals(LongArrayList.from(1), Hppc.subList(from, 0, 1));
    assertEquals(LongArrayList.from(1, 2), Hppc.subList(from, 0, 2));
    assertEquals(LongArrayList.from(2), Hppc.subList(from, 1, 2));
  }
}
