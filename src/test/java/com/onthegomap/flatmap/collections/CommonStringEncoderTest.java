package com.onthegomap.flatmap.collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

public class CommonStringEncoderTest {

  private final CommonStringEncoder commonStringEncoder = new CommonStringEncoder();

  @Test
  public void testRoundTrip() {
    byte a = commonStringEncoder.encode("a");
    byte b = commonStringEncoder.encode("b");
    assertEquals("a", commonStringEncoder.decode(a));
    assertEquals(a, commonStringEncoder.encode("a"));
    assertEquals("b", commonStringEncoder.decode(b));
    assertThrows(IllegalStateException.class, () -> commonStringEncoder.decode((byte) (b + 1)));
  }

  @Test
  public void testLimitsTo250() {
    for (int i = 0; i <= 250; i++) {
      commonStringEncoder.encode(Integer.toString(i));
    }
    assertThrows(IllegalStateException.class, () -> commonStringEncoder.encode("too many"));
  }
}
