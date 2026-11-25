package com.onthegomap.planetiler.util;

import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;

class RepeatedErrorLoggerTest {

  @Test
  void testStackTracesForFirstFewOnly() {
    Logger logger = mock(Logger.class);
    when(logger.isErrorEnabled()).thenReturn(true);
    var errors = new RepeatedErrorLogger(logger, 2);
    var exception = new IllegalStateException("boom");
    for (int i = 1; i <= 4; i++) {
      errors.log("feature " + i, exception);
    }

    var logged = inOrder(logger);
    logged.verify(logger).error("feature 1", exception);
    logged.verify(logger).error("feature 2", exception);
    logged.verify(logger).error("{}: {} (leaving out stack traces after the first {})", "feature 3", exception, 2);
    logged.verify(logger).error("{}: {}", "feature 4", "java.lang.IllegalStateException: boom");
  }
}
