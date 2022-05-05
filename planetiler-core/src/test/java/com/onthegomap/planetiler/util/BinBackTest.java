package com.onthegomap.planetiler.util;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class BinBackTest {

  @ParameterizedTest
  @CsvSource(value = {
    "3;[];[]",
    "2;[1];[[1]]",
    "2;[2];[[2]]",
    "2;[3];[[3]]",
    "3;[1,2,3];[[3], [2, 1]]",
    "5;[1,2,3];[[3,2],[1]]",
    "6;[1,2,3];[[3,2,1]]",
    "2;[1,2,3];[[3],[2],[1]]",
    "1;[1,2,3];[[3],[2],[1]]",
  }, delimiter = ';')
  void test(int limit, String inputString, String expectedString) {
    List<Long> input = parseList(inputString);
    List<List<Long>> expected = parseListList(expectedString);
    // make sure we parsed correctly
    assertEqualsIgnoringWhitespace(inputString, input, "failed to parse input");
    assertEqualsIgnoringWhitespace(expectedString, expected, "failed to parse expected");

    assertEquals(expected, BinPack.pack(input, limit, i -> i));
  }

  private static List<Long> parseList(String string) {
    return Stream.of(string.replaceAll("[\\[\\]]", "").split(","))
      .map(String::strip)
      .filter(s -> !s.isBlank())
      .map(Long::parseLong)
      .toList();
  }

  private static List<List<Long>> parseListList(String string) {
    return Stream.of(string.replaceAll("((^\\[)|(]$))", "").split("]\\s*,\\s*\\["))
      .map(BinBackTest::parseList)
      .filter(l -> !l.isEmpty())
      .toList();
  }

  private static void assertEqualsIgnoringWhitespace(Object expected, Object actual, String message) {
    assertEquals(
      expected.toString().replaceAll("\\s", ""),
      actual.toString().replaceAll("\\s", ""),
      message
    );
  }
}
