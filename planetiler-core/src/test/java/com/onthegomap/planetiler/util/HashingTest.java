package com.onthegomap.planetiler.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import org.junit.jupiter.api.Test;

class HashingTest {

  @Test
  void testFnv32() {
    assertEquals(Hashing.fnv32(), Hashing.fnv32());
    assertEquals(Hashing.fnv32((byte) 1), Hashing.fnv32((byte) 1));
    assertEquals(Hashing.fnv32((byte) 1, (byte) 2), Hashing.fnv32((byte) 1, (byte) 2));
    assertNotEquals(Hashing.fnv32((byte) 1), Hashing.fnv32((byte) 2));
    assertNotEquals(Hashing.fnv32((byte) 1), Hashing.fnv32((byte) 1, (byte) 1));

    assertEquals(Hashing.FNV1_32_INIT, Hashing.fnv32());
    assertEquals(123, Hashing.fnv32(123));
  }

}
