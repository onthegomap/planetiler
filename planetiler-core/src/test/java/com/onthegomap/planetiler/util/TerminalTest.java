package com.onthegomap.planetiler.util;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.OptionalInt;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class TerminalTest {

  @ParameterizedTest
  @CsvSource(nullValues = "null", value = {
    "false, 80, null",
    "true, 80, 80",
    "true, ' 120 ', 120",
    "true, null, null",
    "true, '', null",
    "true, abc, null",
    "true, 0, null",
  })
  void testWidth(boolean isTerminal, String columns, Integer expected) {
    assertEquals(expected == null ? OptionalInt.empty() : OptionalInt.of(expected),
      Terminal.width(isTerminal, columns));
  }
}
