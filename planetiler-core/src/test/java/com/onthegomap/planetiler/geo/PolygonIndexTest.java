package com.onthegomap.planetiler.geo;

import static com.onthegomap.planetiler.TestUtils.assertListsContainSameElements;
import static com.onthegomap.planetiler.TestUtils.newPoint;
import static com.onthegomap.planetiler.TestUtils.rectangle;

import java.util.List;
import org.junit.jupiter.api.Test;

class PolygonIndexTest {

  private final PolygonIndex<Integer> index = PolygonIndex.create();

  @Test
  void testEmpty() {
    assertListsContainSameElements(List.of(), index.getContaining(newPoint(0.5, 0.5)));
    assertListsContainSameElements(List.of(), index.getContainingOrNearest(newPoint(0.5, 0.5)));
  }

  @Test
  void testSingle() {
    index.put(rectangle(0, 1), 1);
    assertListsContainSameElements(List.of(1), index.getContaining(newPoint(0.5, 0.5)));
    assertListsContainSameElements(List.of(1), index.getContainingOrNearest(newPoint(0.5, 0.5)));

    assertListsContainSameElements(List.of(), index.getContaining(newPoint(1.5, 1.5)));
    assertListsContainSameElements(List.of(), index.getContainingOrNearest(newPoint(1.5, 1.5)));
  }

  @Test
  void testMultipleIdentical() {
    index.put(rectangle(0, 1), 1);
    index.put(rectangle(0, 1), 2);
    assertListsContainSameElements(List.of(1, 2), index.getContaining(newPoint(0.5, 0.5)));
    assertListsContainSameElements(List.of(1, 2), index.getContainingOrNearest(newPoint(0.5, 0.5)));
  }

  @Test
  void testMultipleDifferent() {
    index.put(rectangle(0.25, 1), 1);
    index.put(rectangle(0, 1).difference(rectangle(0, 0.5)), 2);

    assertListsContainSameElements(List.of(), index.getContaining(newPoint(0, 0)));
    assertListsContainSameElements(List.of(2), index.getContainingOrNearest(newPoint(0, 0)));

    assertListsContainSameElements(List.of(2, 1), index.getContaining(newPoint(0.75, 0.75)));
    assertListsContainSameElements(List.of(2, 1), index.getContainingOrNearest(newPoint(0.75, 0.75)));
  }
}
