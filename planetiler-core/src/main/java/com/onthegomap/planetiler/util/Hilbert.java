package com.onthegomap.planetiler.util;

/**
 * Fast hilbert space-filling curve implementation ported from C++
 * <a href="https://github.com/rawrunprotected/hilbert_curves">github.com/rawrunprotected/hilbert_curves</a> by
 * <a href="https://threadlocalmutex.com/">threadlocalmutex.com</a> (public domain).
 */
public class Hilbert {
  private Hilbert() {
    throw new IllegalStateException("Utility class");
  }

  private static int deinterleave(int tx) {
    tx = tx & 0x55555555;
    tx = (tx | (tx >>> 1)) & 0x33333333;
    tx = (tx | (tx >>> 2)) & 0x0F0F0F0F;
    tx = (tx | (tx >>> 4)) & 0x00FF00FF;
    tx = (tx | (tx >>> 8)) & 0x0000FFFF;
    return tx;
  }

  private static int interleave(int tx) {
    tx = (tx | (tx << 8)) & 0x00FF00FF;
    tx = (tx | (tx << 4)) & 0x0F0F0F0F;
    tx = (tx | (tx << 2)) & 0x33333333;
    tx = (tx | (tx << 1)) & 0x55555555;
    return tx;
  }

  private static int prefixScan(int tx) {
    tx = (tx >>> 8) ^ tx;
    tx = (tx >>> 4) ^ tx;
    tx = (tx >>> 2) ^ tx;
    tx = (tx >>> 1) ^ tx;
    return tx;
  }

  /** Returns the x coordinate extracted from the result of {@link #hilbertPositionToXY(int, int)}. */
  public static int extractX(long xy) {
    return (int) (xy >>> 32);
  }

  /** Returns the y coordinate extracted from the result of {@link #hilbertPositionToXY(int, int)}. */
  public static int extractY(long xy) {
    return (int) xy;
  }

  /**
   * Returns the x/y coordinates from hilbert index {@code pos} at {@code level} packed into a long.
   *
   * Use {@link #extractX(long)} and {@link #extractY(long)} to extract x and y from the result.
   */
  public static long hilbertPositionToXY(int level, int pos) {
    pos = pos << (32 - 2 * level);

    int i0 = deinterleave(pos);
    int i1 = deinterleave(pos >>> 1);

    int t0 = (i0 | i1) ^ 0xFFFF;
    int t1 = i0 & i1;

    int prefixT0 = prefixScan(t0);
    int prefixT1 = prefixScan(t1);

    int a = (((i0 ^ 0xFFFF) & prefixT1) | (i0 & prefixT0));

    int resultX = (a ^ i1) >>> (16 - level);
    int resultY = (a ^ i0 ^ i1) >>> (16 - level);
    return ((long) resultX << 32) | resultY;
  }

  /** Returns the hilbert index at {@code level} for an x/y coordinate. */
  public static int hilbertXYToIndex(int level, int x, int y) {
    x = x << (16 - level);
    y = y << (16 - level);

    int hA, hB, hC, hD;

    int a1 = x ^ y;
    int b1 = 0xFFFF ^ a1;
    int c1 = 0xFFFF ^ (x | y);
    int d1 = x & (y ^ 0xFFFF);

    hA = a1 | (b1 >>> 1);
    hB = (a1 >>> 1) ^ a1;

    hC = ((c1 >>> 1) ^ (b1 & (d1 >>> 1))) ^ c1;
    hD = ((a1 & (c1 >>> 1)) ^ (d1 >>> 1)) ^ d1;

    int a2 = hA;
    int b2 = hB;
    int c2 = hC;
    int d2 = hD;

    hA = ((a2 & (a2 >>> 2)) ^ (b2 & (b2 >>> 2)));
    hB = ((a2 & (b2 >>> 2)) ^ (b2 & ((a2 ^ b2) >>> 2)));

    hC ^= ((a2 & (c2 >>> 2)) ^ (b2 & (d2 >>> 2)));
    hD ^= ((b2 & (c2 >>> 2)) ^ ((a2 ^ b2) & (d2 >>> 2)));

    int a3 = hA;
    int b3 = hB;
    int c3 = hC;
    int d3 = hD;

    hA = ((a3 & (a3 >>> 4)) ^ (b3 & (b3 >>> 4)));
    hB = ((a3 & (b3 >>> 4)) ^ (b3 & ((a3 ^ b3) >>> 4)));

    hC ^= ((a3 & (c3 >>> 4)) ^ (b3 & (d3 >>> 4)));
    hD ^= ((b3 & (c3 >>> 4)) ^ ((a3 ^ b3) & (d3 >>> 4)));

    int a4 = hA;
    int b4 = hB;
    int c4 = hC;
    int d4 = hD;

    hC ^= ((a4 & (c4 >>> 8)) ^ (b4 & (d4 >>> 8)));
    hD ^= ((b4 & (c4 >>> 8)) ^ ((a4 ^ b4) & (d4 >>> 8)));

    int a = hC ^ (hC >>> 1);
    int b = hD ^ (hD >>> 1);

    int i0 = x ^ y;
    int i1 = b | (0xFFFF ^ (i0 | a));

    return ((interleave(i1) << 1) | interleave(i0)) >>> (32 - 2 * level);
  }
}
