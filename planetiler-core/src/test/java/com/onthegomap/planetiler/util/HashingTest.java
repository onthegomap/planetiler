package com.onthegomap.planetiler.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import java.util.function.Function;
import org.junit.jupiter.api.Test;

class HashingTest {

  @Test
  void testFnv1a32() {
    testHasher(Hashing::fnv1a32, Hashing.FNV1_32_INIT);
    assertEquals(123, Hashing.fnv1a32(123));
  }

  @Test
  void testFnv1a64() {
    testHasher(Hashing::fnv1a64, Hashing.FNV1_64_INIT);
    assertEquals(123, Hashing.fnv1a64(123));
  }

  private static byte[] bytes(int... bytes) {
    byte[] result = new byte[bytes.length];
    for (int i = 0; i < bytes.length; i++) {
      int value = bytes[i];
      assert value >= 0 && value < 256;
      result[i] = (byte) value;
    }
    return result;
  }

  private static <T> void testHasher(Function<byte[], T> hashFn, T init) {
    assertEquals(hashFn.apply(bytes()), hashFn.apply(bytes()));
    assertEquals(hashFn.apply(bytes(1)), hashFn.apply(bytes(1)));
    assertEquals(hashFn.apply(bytes(1, 2)), hashFn.apply(bytes(1, 2)));
    assertNotEquals(hashFn.apply(bytes(1)), hashFn.apply(bytes(2)));
    assertNotEquals(hashFn.apply(bytes(1)), hashFn.apply(bytes(1, 1)));

    assertEquals(init, hashFn.apply(bytes()));
  }
}
