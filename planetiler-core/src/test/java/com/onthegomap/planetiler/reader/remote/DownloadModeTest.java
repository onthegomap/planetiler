package com.onthegomap.planetiler.reader.remote;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class DownloadModeTest {

  @ParameterizedTest
  @CsvSource({
    "false, NONE",
    "none, NONE",
    "true, CACHED",
    "cached, CACHED",
    "Streaming, STREAMING",
    "full, FULL",
  })
  void testParse(String value, DownloadMode expected) {
    assertEquals(expected, DownloadMode.parse(value));
  }

  @Test
  void testRejectsUnknownMode() {
    assertThrows(IllegalArgumentException.class, () -> DownloadMode.parse("sometimes"));
  }
}
