package com.onthegomap.planetiler.util;

public final class Hashing {

  public static final int FNV1_32_INIT = 0x811c9dc5;
  private static final int FNV1_PRIME_32 = 16777619;

  private Hashing() {}

  public static int fnv32(int initHash, byte... data) {
    int hash = initHash;
    for (byte datum : data) {
      hash ^= (datum & 0xff);
      hash *= FNV1_PRIME_32;
    }
    return hash;
  }

  public static int fnv32(byte... data) {
    return fnv32(FNV1_32_INIT, data);
  }

}
