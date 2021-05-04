package com.onthegomap.flatmap.collections;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

public class IntRangeTest {

  private static List<Integer> getInts(IntRange range) {
    List<Integer> result = new ArrayList<>();
    for (Integer integer : range) {
      result.add(integer);
    }
    return result;
  }

  @Test
  public void testEmptyIntRange() {
    IntRange range = new IntRange();
    assertEquals(List.of(), getInts(range));
  }

  @Test
  public void testSingleIntRange() {
    IntRange range = new IntRange();
    range.add(1, 1);
    assertEquals(List.of(1), getInts(range));
  }

  @Test
  public void testLongerIntRange() {
    IntRange range = new IntRange();
    range.add(1, 3);
    assertEquals(List.of(1, 2, 3), getInts(range));
  }

  @Test
  public void testTwoIntRanges() {
    IntRange range = new IntRange();
    range.add(1, 3);
    range.add(5, 7);
    assertEquals(List.of(1, 2, 3, 5, 6, 7), getInts(range));
  }

  @Test
  public void testTwoOverlappingIntRanges() {
    IntRange range = new IntRange();
    range.add(1, 5);
    range.add(4, 7);
    assertEquals(List.of(1, 2, 3, 4, 5, 6, 7), getInts(range));
  }

  @Test
  public void testRemoveSingle() {
    IntRange range = new IntRange();
    range.add(1, 5);
    range.remove(3);
    assertEquals(List.of(1, 2, 4, 5), getInts(range));
  }

  @Test
  public void testRemoveOtherRange() {
    IntRange range = new IntRange();
    range.add(1, 5);
    IntRange range2 = new IntRange();
    range2.add(4, 6);
    range.removeAll(range2);
    assertEquals(List.of(1, 2, 3), getInts(range));
  }

  @Test
  public void testAddOtherRange() {
    IntRange range = new IntRange();
    range.add(1, 5);
    IntRange range2 = new IntRange();
    range2.add(8, 10);
    range.addAll(range2);
    assertEquals(List.of(1, 2, 3, 4, 5, 8, 9, 10), getInts(range));
  }

  @Test
  public void testContains() {
    IntRange range = new IntRange();
    range.add(1, 5);
    IntRange range2 = new IntRange();
    range2.add(8, 10);
    range.addAll(range2);
    range.remove(3);

    var expected = Map.ofEntries(
      Map.entry(1, true),
      Map.entry(2, true),
      Map.entry(3, false),
      Map.entry(4, true),
      Map.entry(5, true),
      Map.entry(6, false),
      Map.entry(7, false),
      Map.entry(8, true),
      Map.entry(9, true),
      Map.entry(10, true),
      Map.entry(11, false)
    );

    expected.forEach((num, val) -> assertEquals(val, range.contains(num), (val ? "" : "!") + num));
  }

  @Test
  public void testNoIntersection() {
    IntRange range = new IntRange();
    range.add(1, 5);
    IntRange range2 = new IntRange();
    range2.add(8, 10);
    range.intersect(range2);
    assertEquals(List.of(), getInts(range));
  }

  @Test
  public void testSingleIntersection() {
    IntRange range = new IntRange();
    range.add(1, 5);
    IntRange range2 = new IntRange();
    range2.add(5, 10);
    range.intersect(range2);
    assertEquals(List.of(5), getInts(range));
  }

  @Test
  public void testMultipleIntersection() {
    IntRange range = new IntRange();
    range.add(1, 5);
    IntRange range2 = new IntRange();
    range2.add(3, 10);
    range2.remove(4);
    range.intersect(range2);
    assertEquals(List.of(3, 5), getInts(range));
  }

  @Test
  public void testNextEmpty() {
    IntRange range = new IntRange();
    assertEquals(1, range.nextInRange(0, 1));
  }

  @Test
  public void testNextInRange() {
    IntRange range = new IntRange();
    range.add(0, 2);
    assertEquals(0, range.nextInRange(0, 1));
  }

  @Test
  public void testNextBelowInRange() {
    IntRange range = new IntRange();
    range.add(2, 4);
    assertEquals(2, range.nextInRange(1, 3));
  }
}
