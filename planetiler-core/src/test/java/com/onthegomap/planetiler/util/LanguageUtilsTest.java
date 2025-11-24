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
    "es",
    "en-US",
    "en-001",
    "fr-x-gallo",
    "ko-Latn",
    "be-tarask",
    "ja-Latn",
    "ja-Hira",
    "vls",
    "zh-hant-CN",
    "zh-Bopo",
    "zh-Latn-tongyong",
    "zh-Latn-pinyin",
    "zh-Latn-wadegile",
    "yue-Latn-jyutping",
    "tec",
    "be-tarask",
    "nan-Latn-pehoeji",
    "zh-Latn-pinyin",
    "en-t-zh",
    "zh-u-nu-hant",
    "en-u-sd-usnc",
    "es-fonipa",
    "fr-x-gallo",
    "i-mingo",
  })
  void testIsValidLanguageTag(String in) {
    assertTrue(LanguageUtils.isValidLanguageTag(in));
  }

  @ParameterizedTest
  @ValueSource(strings = {
    "nombre",
    "",
    "xxxxx",
    "TEC",
    "en-x",
    "ja_rm",
    "ja_kana",
    "zh_pinyin",
    "zh_zhuyin",
    "zh-Latn-wadegiles",
    "etymology",
    "etymology:wikidata",
  })
  void testIsNotValidLanguageTag(String in) {
    assertFalse(LanguageUtils.isValidLanguageTag(in));
  }

  @ParameterizedTest
  @ValueSource(strings = {
    "name:tlh",
  })
  void testIsValidOsmNameTag(String in) {
    assertTrue(LanguageUtils.isValidOsmNameTag(in));
  }

  @ParameterizedTest
  @ValueSource(strings = {
    "name",
    "name:",
    "name:TEC",
    "official_name:en-US",
  })
  void testIsNotValidOsmNameTag(String in) {
    assertFalse(LanguageUtils.isValidOsmNameTag(in));
  }

}
