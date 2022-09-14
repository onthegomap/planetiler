package com.onthegomap.planetiler.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

class TryTest {
  @Test
  void success() {
    var result = Try.apply(() -> 1);
    assertEquals(Try.success(1), result);
    assertEquals(1, result.item());
  }

  @Test
  void failure() {
    var exception = new IllegalStateException();
    var result = Try.apply(() -> {
      throw exception;
    });
    assertEquals(Try.failure(exception), result);
    assertThrows(IllegalStateException.class, result::item);
  }
}
