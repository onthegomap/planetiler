package com.onthegomap.planetiler.reader.osm;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.carrotsearch.hppc.IntArrayList;
import com.carrotsearch.hppc.LongArrayList;
import org.junit.jupiter.api.Test;

abstract class OsmWaySplitterTest {
  protected OsmWaySplitter splitter = get();

  abstract OsmWaySplitter get();

  private void addWay(long... nodeIds) {
    try (var writer = splitter.writerForThread()) {
      writer.addWay(LongArrayList.from(nodeIds));
    }
  }

  private IntArrayList split(long... nodeIds) {
    return splitter.getSplitIndices(LongArrayList.from(nodeIds));
  }

  @Test
  void testSplitWhenEmpty() {
    assertEquals(IntArrayList.from(), split());
    assertEquals(IntArrayList.from(), split(1));
    assertEquals(IntArrayList.from(), split(1, 2));
  }

  @Test
  void testSplitNoIntersection() {
    addWay(1, 2);
    assertEquals(IntArrayList.from(), split());
    assertEquals(IntArrayList.from(), split(1));
    assertEquals(IntArrayList.from(), split(1, 2));
  }

  @Test
  void testSplitWithIntersection() {
    addWay(1, 2, 3);
    addWay(2, 4);
    assertEquals(IntArrayList.from(1), split(1, 2, 3));
    assertEquals(IntArrayList.from(), split(2, 4));
  }

  @Test
  void testSplitIgnoresEndpoints() {
    addWay(1, 2, 3);
    addWay(1, 4);
    addWay(3, 4);
    assertEquals(IntArrayList.from(), split(1, 2, 3));
    assertEquals(IntArrayList.from(), split(1, 4));
    assertEquals(IntArrayList.from(), split(3, 4));
  }

  @Test
  void testSplitWithMultipleIntersections() {
    addWay(1, 2, 3, 4, 5, 3);
    addWay(1, 11);
    addWay(2, 12);
    addWay(4, 14);
    addWay(5, 15);
    assertEquals(IntArrayList.from(1, 2, 3, 4), split(1, 2, 3, 4, 5, 3));
    assertEquals(IntArrayList.from(), split(1, 11));
    assertEquals(IntArrayList.from(), split(2, 12));
    assertEquals(IntArrayList.from(), split(4, 14));
    assertEquals(IntArrayList.from(), split(5, 15));
  }

  @Test
  void testSplitWithinPartition() {
    try (var writer = splitter.writerForThread()) {
      writer.addWay(LongArrayList.from(1, 2, 3, 4));
      writer.addWay(LongArrayList.from(2, 5));
    }
    assertEquals(IntArrayList.from(1), split(1, 2, 3, 4));
  }

  @Test
  void testBigNumbers() {
    long base = 5_000_000_000L;
    try (var writer = splitter.writerForThread()) {
      writer.addWay(LongArrayList.from(base + 1, base + 2, base + 3));
      writer.addWay(LongArrayList.from(base + 2, base + 4, base + 5));
    }
    addWay(base + 4, base + 6);
    assertEquals(IntArrayList.from(1), split(base + 1, base + 2, base + 3));
    assertEquals(IntArrayList.from(1), split(base + 2, base + 4, base + 5));
  }

  @Test
  void testSelfLoop() {
    addWay(1, 2, 3, 4, 2, 5);
    assertEquals(IntArrayList.from(1, 4), split(1, 2, 3, 4, 2, 5));
  }

  static class MapSplitterTest extends OsmWaySplitterTest {

    @Override
    OsmWaySplitter get() {
      return OsmWaySplitter.mapSplitter();
    }
  }

  static class RoaringBitmapSplitterTest extends OsmWaySplitterTest {

    @Override
    OsmWaySplitter get() {
      return OsmWaySplitter.roaringBitmapSplitter();
    }
  }

}
