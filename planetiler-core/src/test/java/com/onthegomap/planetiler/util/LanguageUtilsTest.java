package com.onthegomap.planetiler.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

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
  @ValueSource(strings = {
    "name:es",
    "name:en-US",
    "name:en-001",
    "name:fr-x-gallo",
    "name:ko-Latn",
    "name:be-tarask",
    "name:ja_rm",
    "name:ja_kana",
    "name:vls",
    "name:zh-hant-CN",
    "name:zh_pinyin",
    "name:zh_zhuyin",
    "name:zh-Latn-tongyong",
    "name:zh-Latn-pinyin",
    "name:zh-Latn-wadegiles",
    "name:yue-Latn-jyutping",
    "name:tec",
    "name:be-tarask",
    "name:nan-Latn-pehoeji",
    "name:zh-Latn-pinyin",
  })
  void testIsValidOsmNameTag(String in) {
    assertTrue(LanguageUtils.isValidOsmNameTag(in));
  }

  @ParameterizedTest
  @ValueSource(strings = {
    "nombre",
    "name:",
    "name:xxxxx",
    "name:TEC",
  })
  void testIsNotValidOsmNameTag(String in) {
    assertFalse(LanguageUtils.isValidOsmNameTag(in));
  }

}
