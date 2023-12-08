package com.onthegomap.planetiler.experimental.lua;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class JavaToLuaCaseTest {
  @ParameterizedTest
  @CsvSource({
    "foo, foo",
    "Foo, Foo",
    "fooBar, foo_bar",
    "FooBar, FooBar",
    "foo_bar, foo_bar",
    "foo_BAR, foo_BAR",
    "FOO_BAR, FOO_BAR",
    "fooBAR, foo_bar",
    "getISO3Code, get_iso3_code",
    "utf8string, utf8_string",
    "arg0, arg0",
    "arg10, arg10",
    "utf8String, utf8_string",
    "getUTF8String, get_utf8_string",
    "getUTF8string, get_utf8_string",
    "getUTF8ASCIIString, get_utf8ascii_string",
    "iso31661Alpha2, iso31661_alpha2",
    "getUTF8At0, get_utf8_at0",
    "distance3D, distance3d",
    "toASCIIString, to_ascii_string",
    "and, AND",
  })
  void testConversions(String input, String expectedOutput) {
    assertEquals(expectedOutput, JavaToLuaCase.transformMemberName(input));
  }
}
