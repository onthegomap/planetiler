package com.onthegomap.planetiler.util;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
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

  @Test
  void testUnwrap() {
    var cause = new IllegalStateException("cause");
    assertSame(cause, Exceptions.unwrap(cause));
    assertSame(cause, Exceptions.unwrap(new CompletionException(new ExecutionException(cause))));
    assertSame(cause, Exceptions.unwrap(new Exceptions.FatalPlanetilerException(new ExecutionException(cause))));
    var withoutCause = new CompletionException("no cause", null);
    assertSame(withoutCause, Exceptions.unwrap(withoutCause));
    var notAWrapper = new UncheckedIOException(new IOException("io"));
    assertSame(notAWrapper, Exceptions.unwrap(notAWrapper));
  }

  @Test
  void testHasCause() {
    var exception = new RuntimeException(new ExecutionException(new IOException("disk full")));
    assertTrue(Exceptions.hasCause(exception, IOException.class));
    assertTrue(Exceptions.hasCause(exception, RuntimeException.class));
    assertFalse(Exceptions.hasCause(exception, UncheckedIOException.class));
    assertFalse(Exceptions.hasCause(null, IOException.class));
  }
}
