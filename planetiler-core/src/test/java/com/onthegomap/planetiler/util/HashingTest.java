package com.onthegomap.planetiler.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import org.junit.jupiter.api.Test;

class HashingTest {

  @Test
  void testFnv1a32() {
    assertEquals(Hashing.fnv1a32(), Hashing.fnv1a32());
    assertEquals(Hashing.fnv1a32((byte) 1), Hashing.fnv1a32((byte) 1));
    assertEquals(Hashing.fnv1a32((byte) 1, (byte) 2), Hashing.fnv1a32((byte) 1, (byte) 2));
    assertNotEquals(Hashing.fnv1a32((byte) 1), Hashing.fnv1a32((byte) 2));
    assertNotEquals(Hashing.fnv1a32((byte) 1), Hashing.fnv1a32((byte) 1, (byte) 1));

    assertEquals(Hashing.FNV1_32_INIT, Hashing.fnv1a32());
    assertEquals(123, Hashing.fnv1a32(123));
  }

}
