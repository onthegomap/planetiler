package com.onthegomap.planetiler.geo;

import static com.onthegomap.planetiler.TestUtils.assertListsContainSameElements;
import static com.onthegomap.planetiler.TestUtils.newPoint;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.List;
import org.junit.jupiter.api.Test;

public class PointIndexTest {

  private final PointIndex<Integer> index = PointIndex.create();

  @Test
  public void testEmpty() {
    assertListsContainSameElements(
      List.of(),
      index.getWithin(newPoint(0.5, 0.5), 1)
    );
    assertNull(
      index.getNearest(newPoint(0.5, 0.5), 1)
    );
  }

  @Test
  public void testSingle() {
    index.put(newPoint(0.5, 1), 1);
    assertListsContainSameElements(
      List.of(1),
      index.getWithin(newPoint(0.5, 0.5), 0.6)
    );
    assertListsContainSameElements(
      List.of(),
      index.getWithin(newPoint(0.5, 0.5), 0.4)
    );
    assertNull(index.getNearest(newPoint(0.5, 0.5), 0.4));
    assertEquals(1, index.getNearest(newPoint(0.5, 0.5), 0.6));
  }

  @Test
  public void testMultipleIdentical() {
    index.put(newPoint(1, 1), 1);
    index.put(newPoint(1, 1), 2);
    assertListsContainSameElements(
      List.of(1, 2),
      index.getWithin(newPoint(0.5, 1), 0.6)
    );
    assertListsContainSameElements(
      List.of(),
      index.getWithin(newPoint(0.5, 1), 0.4)
    );
  }

  @Test
  public void testMultipleDifferent() {
    index.put(newPoint(0, 1), 1);
    index.put(newPoint(1, 1), 2);
    assertListsContainSameElements(
      List.of(1, 2),
      index.getWithin(newPoint(0.5, 1), 0.6)
    );
    assertListsContainSameElements(
      List.of(1),
      index.getWithin(newPoint(0.4, 1), 0.4)
    );
    assertListsContainSameElements(
      List.of(2),
      index.getWithin(newPoint(0.6, 1), 0.4)
    );
    assertListsContainSameElements(
      List.of(),
      index.getWithin(newPoint(0.5, 1), 0.4)
    );

    assertEquals(1, index.getNearest(newPoint(0.4, 1), 0.5));
    assertEquals(1, index.getNearest(newPoint(0.4, 1), 1));

    assertNull(index.getNearest(newPoint(0.5, 1), 0.4));

    assertEquals(2, index.getNearest(newPoint(0.6, 1), 0.5));
    assertEquals(2, index.getNearest(newPoint(0.6, 1), 1));
  }
}
