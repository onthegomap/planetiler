package com.onthegomap.planetiler.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

class CommonStringEncoderTest {

  private final CommonStringEncoder commonStringEncoder = new CommonStringEncoder();

  @Test
  void testRoundTrip() {
    byte a = commonStringEncoder.encodeByte("a");
    byte b = commonStringEncoder.encodeByte("b");
    assertEquals("a", commonStringEncoder.decodeByte(a));
    assertEquals(a, commonStringEncoder.encodeByte("a"));
    assertEquals("b", commonStringEncoder.decodeByte(b));
    assertThrows(IllegalArgumentException.class, () -> commonStringEncoder.decodeByte((byte) (b + 1)));
  }

  @Test
  void testLimitsTo250() {
    for (int i = 0; i <= 250; i++) {
      String string = Integer.toString(i);
      byte encoded = commonStringEncoder.encodeByte(Integer.toString(i));
      String decoded = commonStringEncoder.decodeByte(encoded);
      assertEquals(string, decoded);
    }
    assertThrows(IllegalArgumentException.class, () -> commonStringEncoder.encodeByte("too many"));
  }
}
