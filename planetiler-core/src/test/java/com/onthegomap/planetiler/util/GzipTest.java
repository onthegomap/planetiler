package com.onthegomap.planetiler.util;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import java.io.IOException;
import java.util.Arrays;
import org.junit.jupiter.api.Test;

public class GzipTest {

  @Test
  public void testRoundTrip() throws IOException {
    String string = "abcdef";
    byte[] small = Gzip.gzip(string.getBytes(UTF_8));
    byte[] big = Gzip.gunzip(small);
    assertEquals(string, new String(big, UTF_8));
    assertFalse(Arrays.equals(small, big));
  }
}
