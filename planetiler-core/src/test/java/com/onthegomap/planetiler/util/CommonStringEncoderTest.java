package com.onthegomap.planetiler.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

class CommonStringEncoderTest {

  private final CommonStringEncoder commonStringEncoderInteger = new CommonStringEncoder(100_000);
  private final CommonStringEncoder.AsByte commonStringEncoderByte = new CommonStringEncoder.AsByte();

  @Test
  void testRoundTripByte() {
    byte a = commonStringEncoderByte.encode("a");
    byte b = commonStringEncoderByte.encode("b");
    assertEquals("a", commonStringEncoderByte.decode(a));
    assertEquals(a, commonStringEncoderByte.encode("a"));
    assertEquals("b", commonStringEncoderByte.decode(b));
    assertThrows(IllegalArgumentException.class, () -> commonStringEncoderByte.decode((byte) (b + 1)));
  }

  @Test
  void testRoundTripInteger() {
    int a = commonStringEncoderInteger.encode("a");
    int b = commonStringEncoderInteger.encode("b");
    assertEquals("a", commonStringEncoderInteger.decode(a));
    assertEquals(a, commonStringEncoderInteger.encode("a"));
    assertEquals("b", commonStringEncoderInteger.decode(b));
    assertThrows(IllegalArgumentException.class, () -> commonStringEncoderInteger.decode(b + 1));
  }

  @Test
  void testByteLimitsToMax() {
    for (int i = 0; i <= 255; i++) {
      String string = Integer.toString(i);
      byte encoded = commonStringEncoderByte.encode(Integer.toString(i));
      String decoded = commonStringEncoderByte.decode(encoded);
      assertEquals(string, decoded);
    }
    assertThrows(IllegalArgumentException.class, () -> commonStringEncoderByte.encode("too many"));
  }
}
