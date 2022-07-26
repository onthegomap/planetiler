package com.onthegomap.planetiler.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
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

  @ParameterizedTest
  @CsvSource({
    "0,0,0,0",

    "1,0,0,0",
    "1,0,1,1",
    "1,1,1,2",
    "1,1,0,3",

    "2,1,1,2",

    "15,0,0,0",
    "15,0,1,1",
    "15,1,1,2",
    "15,1,0,3",
    "15,32767,0,1073741823",
    "15,32767,32767,715827882",

    "16,0,0,0",
    "16,1,0,1",
    "16,1,1,2",
    "16,0,1,3",
    "16,65535,0,-1",
    "16,65535,65535,-1431655766",
  })
  void testEncoding(int level, int x, int y, int encoded) {
    int actualEncoded = Hilbert.hilbertXYToIndex(level, x, y);
    assertEquals(encoded, actualEncoded);
    long decoded = Hilbert.hilbertPositionToXY(level, encoded);
    assertEquals(x, Hilbert.extractX(decoded));
    assertEquals(y, Hilbert.extractY(decoded));
  }
}
