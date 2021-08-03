package com.onthegomap.flatmap;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

public class FormatTest {

  @ParameterizedTest
  @CsvSource({
    "1.5,1",
    "999,999",
    "1000,1k",
    "9999,9.9k",
    "10001,10k",
    "99999,99k",
    "999999,999k",
    "9999999,9.9M",
    "-9999999,-9.9M",
    "5.5e12,5.5T",
  })
  public void testFormatNumeric(Double number, String formatted) {
    assertEquals(Format.formatNumeric(number, false), formatted);
  }

  @ParameterizedTest
  @CsvSource({
    "999,999",
    "1000,1kB",
    "9999,9.9kB",
    "5.5e9,5.5GB",
  })
  public void testFormatStorage(Double number, String formatted) {
    assertEquals(formatted, Format.formatStorage(number, false));
  }

  @ParameterizedTest
  @CsvSource({
    "0,0%",
    "1,100%",
    "0.11111,11%",
  })
  public void testFormatPercent(Double number, String formatted) {
    assertEquals(formatted, Format.formatPercent(number));
  }

  @ParameterizedTest
  @CsvSource({
    "a,0,a",
    "a,1,a",
    "a,2,' a'",
    "a,3,'  a'",
    "ab,3,' ab'",
    "abc,3,'abc'",
  })
  public void testPad(String in, Integer size, String out) {
    assertEquals(out, Format.padLeft(in, size));
  }

  @ParameterizedTest
  @CsvSource({
    "0,0",
    "0.1,0.1",
    "0.11,0.1",
    "1111.11,'1,111.1'",
  })
  public void testFormatDecimal(Double in, String out) {
    assertEquals(out, Format.formatDecimal(in));
  }
}
