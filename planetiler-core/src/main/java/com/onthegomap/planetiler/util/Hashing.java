package com.onthegomap.planetiler.util;

public final class Hashing {

  private static final int FNV1_32_INIT = 0x811c9dc5;
  private static final int FNV1_PRIME_32 = 16777619;

  private Hashing() {}

  public static int fnv32(byte[] data) {
    int hash = FNV1_32_INIT;
    for (byte datum : data) {
      hash ^= (datum & 0xff);
      hash *= FNV1_PRIME_32;
    }
    return hash;
  }

}
