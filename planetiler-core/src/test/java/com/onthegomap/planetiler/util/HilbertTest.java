package com.onthegomap.planetiler.util;

import static org.junit.jupiter.api.Assertions.fail;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class HilbertTest {
  @ParameterizedTest
  @ValueSource(ints = {0, 1, 2, 3, 4, 5, 15})
  void testRoundTrip(int level) {
    int max = (1 << level) * (1 << level);
    int step = Math.max(1, max / 100);
    for (int i = 0; i < max; i += step) {
      long decoded = Hilbert.hilbertPositionToXY(level, i);
      int x = Hilbert.extractX(decoded);
      int y = Hilbert.extractY(decoded);
      int reEncoded = Hilbert.hilbertXYToIndex(level, x, y);
      if (reEncoded != i) {
        fail("x=" + x + ", y=" + y + " index=" + i + " re-encoded=" + reEncoded);
      }
    }
  }
}
