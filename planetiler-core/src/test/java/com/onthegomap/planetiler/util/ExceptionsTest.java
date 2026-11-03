package com.onthegomap.planetiler.util;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.io.UncheckedIOException;
import org.junit.jupiter.api.Test;

class ExceptionsTest {

  @Test
  void testThrowAsIOException() {
    var io = new IOException("io");
    assertSame(io, assertThrows(IOException.class, () -> Exceptions.throwAsIOException(io)));
    var runtime = new IllegalArgumentException("runtime");
    assertSame(runtime, assertThrows(IllegalArgumentException.class, () -> Exceptions.throwAsIOException(runtime)));
    var error = new AssertionError("error");
    assertSame(error, assertThrows(AssertionError.class, () -> Exceptions.throwAsIOException(error)));
    var unchecked = new UncheckedIOException(io);
    assertSame(io, assertThrows(IOException.class, () -> Exceptions.throwAsIOException(unchecked)));
    var checked = new Exception("checked");
    assertSame(checked, assertThrows(Exceptions.FatalPlanetilerException.class,
      () -> Exceptions.throwAsIOException(checked)).getCause());
  }
}
