package com.onthegomap.planetiler.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.RETURNS_SELF;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.onthegomap.planetiler.reader.FileFormatException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.AccessDeniedException;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.spi.LoggingEventBuilder;

class FailureReportTest {

  @Test
  void testBadOptionsOrInputs() {
    assertEquals("bad option", FailureReport.expectedFailureMessage(new IllegalArgumentException("bad option")));
    assertEquals("bad file", FailureReport.expectedFailureMessage(
      new RuntimeException("Error occurred during stage osm", new FileFormatException("bad file"))));
    assertEquals("java.lang.IllegalArgumentException (run with --verbose for details)",
      FailureReport.expectedFailureMessage(new IllegalArgumentException()));
  }

  @Test
  void testIllegalArgumentFromWorkerIsBug() {
    assertNull(FailureReport.expectedFailureMessage(
      new RuntimeException(new ExecutionException(new IllegalArgumentException("bug")))));
    assertNull(FailureReport.expectedFailureMessage(new CompletionException(new IllegalArgumentException("bug"))));
  }

  @Test
  void testOtherExceptionsAreBugs() {
    assertNull(FailureReport.expectedFailureMessage(new IllegalStateException("bug")));
    assertNull(FailureReport.expectedFailureMessage(new AssertionError("bug")));
  }

  @Test
  void testOutOfMemory() {
    String message = FailureReport.expectedFailureMessage(
      new RuntimeException(new ExecutionException(new OutOfMemoryError("Java heap space"))));
    assertTrue(message.startsWith("Ran out of memory (current limit is "), message);
  }

  @Test
  void testInsufficientResources() {
    assertEquals("Not enough disk space", FailureReport.expectedFailureMessage(
      new CompletionException(new InsufficientResourcesException("Not enough disk space"))));
  }

  @Test
  void testIOErrors() {
    assertEquals("Connection reset", FailureReport.expectedFailureMessage(
      new ExecutionException(new UncheckedIOException(new IOException("Connection reset")))));
    assertEquals("Permission denied: data/tmp",
      FailureReport.expectedFailureMessage(new UncheckedIOException(new AccessDeniedException("data/tmp"))));
  }

  @Test
  void testBugIsLoggedWithStackTrace() {
    Logger logger = mock(Logger.class);
    LoggingEventBuilder event = mock(LoggingEventBuilder.class, RETURNS_SELF);
    when(logger.atError()).thenReturn(event);
    when(logger.atInfo()).thenReturn(event);
    var bug = new IllegalStateException("bug");

    FailureReport.log(bug, false, logger);

    verify(event).setCause(bug);
  }

  @Test
  void testExpectedFailureIsOneLine() {
    Logger logger = mock(Logger.class);
    var failure = new IllegalArgumentException("bad option");

    FailureReport.log(failure, false, logger);
    verify(logger).error("bad option");
    FailureReport.log(failure, true, logger);
    verify(logger).error("bad option", failure);
  }
}
