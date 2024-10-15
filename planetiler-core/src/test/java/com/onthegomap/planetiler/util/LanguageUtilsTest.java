package com.onthegomap.planetiler.util;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class LanguageUtilsTest {

  @ParameterizedTest
  @CsvSource(value = {
    "null,null",
    "abcaāíìś+, +",
    "abcaāíìś, null",
    "日本, 日本",
    "abca日āíìś+, 日+",
    "(abc), null",
    "日本 (Japan), 日本",
    "日本 [Japan - Nippon], 日本",
    "  Japan - Nippon (Japan) - Japan - 日本 - Japan - Nippon (Japan), 日本",
    "Japan - 日本~+  , 日本~+",
    "Japan / 日本 / Japan  , 日本",
  }, nullValues = "null")
  void testRemoveNonLatin(String in, String out) {
    assertEquals(out, LanguageUtils.removeLatinCharacters(in));
  }

  @ParameterizedTest
  @CsvSource(value = {
    "name:es, true",
    "name:en-US, true",
    "name:fr-x-gallo, true",
    "name:ko-Latn, true",
    "name:be-tarask, true",
    "name:ja_rm, true",
    "name:ja_kana, true",
    "name:vls, true",
    "name:zh-hant-CN, true",
    "name:zh_pinyin, true",
    "name:zh_zhuyin, true",
    "name:zh-Latn-tongyong, true",
    "name:zh-Latn-pinyin, true",
    "name:zh-Latn-wadegiles, true",
    "name:yue-Latn-jyutping, true",
    "nombre, false",
    "name:, false",
    "name:xxxxx, false",
  }, nullValues = "null")
  void testIsValidOsmNameTag(String in, boolean out) {
    assertEquals(out, LanguageUtils.isValidOsmNameTag(in));
  }

}
