package com.onthegomap.planetiler.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class TryTest {
  @Test
  void success() {
    var result = Try.apply(() -> 1);
    assertEquals(Try.success(1), result);
    assertEquals(1, result.get());
  }

  @Test
  void failure() {
    var exception = new IllegalStateException();
    var result = Try.apply(() -> {
      throw exception;
    });
    assertEquals(Try.failure(exception), result);
    assertThrows(IllegalStateException.class, result::get);
  }

  @Test
  void cast() {
    var result = Try.apply(() -> 1);
    assertEquals(Try.success(1), result.cast(Number.class));
    assertTrue(result.cast(String.class).isFailure());
    assertInstanceOf(ClassCastException.class, result.cast(String.class).exception());
  }
}
