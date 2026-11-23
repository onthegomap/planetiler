package com.onthegomap.planetiler.util;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class TerminalTest {

  @Test
  void testNoWidthWhenOutputIsNotATerminal() {
    // tests run with output going to a pipe or file
    assertEquals(Optional.empty(), Terminal.width());
  }

  @ParameterizedTest
  @CsvSource(nullValues = "null", value = {
    "80, 80",
    "' 120 ', 120",
    "null, null",
    "'', null",
    "abc, null",
    "0, null",
  })
  void testParseColumns(String columns, Integer expected) {
    assertEquals(Optional.ofNullable(expected), Terminal.parseColumns(columns));
  }
}
