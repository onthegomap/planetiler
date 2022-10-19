package com.onthegomap.planetiler.collection;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class SortableFeatureTest {

  @Test
  void testDifferentKeyDifferentValue() {
    final SortableFeature SF1 = new SortableFeature(0, new byte[]{0, 1});
    final SortableFeature SF2 = new SortableFeature(1, new byte[]{2, 3});
    assertNotEquals(SF1, SF2);
    assertTrue(SF1.compareTo(SF2) < 0);
    assertFalse(SF1.compareTo(SF2) == 0);
    assertFalse(SF2.compareTo(SF1) < 0);
  }

  @Test
  void testDifferentKeySameValue() {
    final SortableFeature SF1 = new SortableFeature(0, new byte[]{0, 1});
    final SortableFeature SF2 = new SortableFeature(1, new byte[]{0, 1});
    assertNotEquals(SF1, SF2);
    assertTrue(SF1.compareTo(SF2) < 0);
    assertFalse(SF1.compareTo(SF2) == 0);
    assertFalse(SF2.compareTo(SF1) < 0);
  }

  @Test
  void testSameKeyDifferentValue() {
    final SortableFeature SF1 = new SortableFeature(0, new byte[]{0, 1});
    final SortableFeature SF2 = new SortableFeature(0, new byte[]{2, 3});
    assertNotEquals(SF1, SF2);
    assertTrue(SF1.compareTo(SF2) < 0);
    assertFalse(SF1.compareTo(SF2) == 0);
    assertFalse(SF2.compareTo(SF1) < 0);
  }

  @Test
  void testSameKeySameValue() {
    final SortableFeature SF1 = new SortableFeature(0, new byte[]{0, 1});
    final SortableFeature SF2 = new SortableFeature(0, new byte[]{0, 1});
    assertEquals(SF1, SF2);
    assertFalse(SF1.compareTo(SF2) < 0);
    assertTrue(SF1.compareTo(SF2) == 0);
    assertFalse(SF2.compareTo(SF1) < 0);
  }
}
