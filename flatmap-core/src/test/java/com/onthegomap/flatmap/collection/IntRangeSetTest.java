package com.onthegomap.flatmap.collection;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

public class IntRangeSetTest {

  private static List<Integer> getInts(IntRangeSet range) {
    List<Integer> result = new ArrayList<>();
    for (Integer integer : range) {
      result.add(integer);
    }
    return result;
  }

  @Test
  public void testEmptyIntRange() {
    IntRangeSet range = new IntRangeSet();
    assertEquals(List.of(), getInts(range));
  }

  @Test
  public void testSingleIntRange() {
    IntRangeSet range = new IntRangeSet();
    range.add(1, 1);
    assertEquals(List.of(1), getInts(range));
  }

  @Test
  public void testLongerIntRange() {
    IntRangeSet range = new IntRangeSet();
    range.add(1, 3);
    assertEquals(List.of(1, 2, 3), getInts(range));
  }

  @Test
  public void testTwoIntRanges() {
    IntRangeSet range = new IntRangeSet();
    range.add(1, 3);
    range.add(5, 7);
    assertEquals(List.of(1, 2, 3, 5, 6, 7), getInts(range));
  }

  @Test
  public void testTwoOverlappingIntRanges() {
    IntRangeSet range = new IntRangeSet();
    range.add(1, 5);
    range.add(4, 7);
    assertEquals(List.of(1, 2, 3, 4, 5, 6, 7), getInts(range));
  }

  @Test
  public void testRemoveSingle() {
    IntRangeSet range = new IntRangeSet();
    range.add(1, 5);
    range.remove(3);
    assertEquals(List.of(1, 2, 4, 5), getInts(range));
  }

  @Test
  public void testRemoveOtherRange() {
    IntRangeSet range = new IntRangeSet();
    range.add(1, 5);
    IntRangeSet range2 = new IntRangeSet();
    range2.add(4, 6);
    range.removeAll(range2);
    assertEquals(List.of(1, 2, 3), getInts(range));
  }

  @Test
  public void testAddOtherRange() {
    IntRangeSet range = new IntRangeSet();
    range.add(1, 5);
    IntRangeSet range2 = new IntRangeSet();
    range2.add(8, 10);
    range.addAll(range2);
    assertEquals(List.of(1, 2, 3, 4, 5, 8, 9, 10), getInts(range));
  }

  @Test
  public void testContains() {
    IntRangeSet range = new IntRangeSet();
    range.add(1, 5);
    IntRangeSet range2 = new IntRangeSet();
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
    IntRangeSet range = new IntRangeSet();
    range.add(1, 5);
    IntRangeSet range2 = new IntRangeSet();
    range2.add(8, 10);
    range.intersect(range2);
    assertEquals(List.of(), getInts(range));
  }

  @Test
  public void testSingleIntersection() {
    IntRangeSet range = new IntRangeSet();
    range.add(1, 5);
    IntRangeSet range2 = new IntRangeSet();
    range2.add(5, 10);
    range.intersect(range2);
    assertEquals(List.of(5), getInts(range));
  }

  @Test
  public void testMultipleIntersection() {
    IntRangeSet range = new IntRangeSet();
    range.add(1, 5);
    IntRangeSet range2 = new IntRangeSet();
    range2.add(3, 10);
    range2.remove(4);
    range.intersect(range2);
    assertEquals(List.of(3, 5), getInts(range));
  }
}
