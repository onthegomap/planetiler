package com.onthegomap.planetiler.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

class CommonStringEncoderTest {

  private final CommonStringEncoder commonStringEncoder = new CommonStringEncoder();

  @Test
  void testRoundTrip() {
    byte a = commonStringEncoder.encode("a");
    byte b = commonStringEncoder.encode("b");
    assertEquals("a", commonStringEncoder.decode(a));
    assertEquals(a, commonStringEncoder.encode("a"));
    assertEquals("b", commonStringEncoder.decode(b));
    assertThrows(IllegalArgumentException.class, () -> commonStringEncoder.decode((byte) (b + 1)));
  }

  @Test
  void testLimitsTo250() {
    for (int i = 0; i <= 250; i++) {
      String string = Integer.toString(i);
      byte encoded = commonStringEncoder.encode(Integer.toString(i));
      String decoded = commonStringEncoder.decode(encoded);
      assertEquals(string, decoded);
    }
    assertThrows(IllegalArgumentException.class, () -> commonStringEncoder.encode("too many"));
  }
}
