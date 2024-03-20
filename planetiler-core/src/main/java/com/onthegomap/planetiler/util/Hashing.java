package com.onthegomap.planetiler.util;

/**
 * Static hash functions and hashing utilities.
 *
 */
public final class Hashing {

  /**
   * Initial hash for the FNV-1 and FNV-1a 32-bit hash function.
   */
  public static final int FNV1_32_INIT = 0x811c9dc5;
  private static final int FNV1_PRIME_32 = 16777619;

  /**
   * Initial hash for the FNV-1 and FNV-1a 64-bit hash function.
   */
  public static final long FNV1_64_INIT = 0xcbf29ce484222325L;
  private static final long FNV1_PRIME_64 = 1099511628211L;

  private Hashing() {}

  /**
   * Computes the hash using the FNV-1a 32-bit hash function, starting with the initial hash.
   * <p>
   * The hash generation must always start with {@link #FNV1_32_INIT} as initial hash but this version comes in handy
   * when generating the hash for multiple bytes consecutively in a loop.
   *
   * @param initHash the initial hash
   * @param data     the data to generate the hash for
   * @return the generated hash
   */
  public static int fnv1a32(int initHash, byte... data) {
    int hash = initHash;
    for (byte datum : data) {
      hash ^= (datum & 0xff);
      hash *= FNV1_PRIME_32;
    }
    return hash;
  }

  /**
   * Computes the hash using the FNV-1a 32-bit hash function.
   *
   * @param data the data to generate the hash for
   * @return the hash
   */
  public static int fnv1a32(byte... data) {
    return fnv1a32(FNV1_32_INIT, data);
  }

  /**
   * Computes the hash using the FNV-1a 64-bit hash function, starting with the initial hash.
   * <p>
   * The hash generation must always start with {@link #FNV1_64_INIT} as initial hash but this version comes in handy
   * when generating the hash for multiple bytes consecutively in a loop.
   *
   * @param initHash the initial hash
   * @param data     the data to generate the hash for
   * @return the generated hash
   */
  public static long fnv1a64(long initHash, byte... data) {
    long hash = initHash;
    if (data == null) {
      return hash;
    }
    for (byte datum : data) {
      hash ^= (datum & 0xff);
      hash *= FNV1_PRIME_64;
    }
    return hash;
  }

  /**
   * Computes the hash using the FNV-1a 64-bit hash function.
   *
   * @param data the data to generate the hash for
   * @return the hash
   */
  public static long fnv1a64(byte... data) {
    return fnv1a64(FNV1_64_INIT, data);
  }

}
