package com.onthegomap.planetiler.util;

import static com.onthegomap.planetiler.util.LanguageUtils.containsOnlyLatinCharacters;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

class LanguageUtilsTest {

  @ParameterizedTest
  @CsvSource({
    "abc, true",
    "5!, true",
    "5~, true",
    "é, true",
    "éś, true",
    "ɏə, true",
    "ɐ, true",
    "ᵿἀ, false",
    "Ḁỿ, true",
    "\u02ff\u0370, false",
    "\u0030\u036f, true",
    "日本, false",
    "abc本123, false",
  })
  void testIsLatin(String in, boolean isLatin) {
    if (!isLatin) {
      assertFalse(containsOnlyLatinCharacters(in));
    } else {
      assertEquals(in, LanguageUtils.getLatinName(Map.of(
        "name", in
      ), true));
    }
  }

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
    // OSM tags that SHOULD be eligible for name:latin feature in the output
    "name:en",
    "name:en-US",
    "name:en-010",
    "int_name",
    "name:fr",
    "name:es",
    "name:pt",
    "name:de",
    "name:ar",
    "name:it",
    "name:ko-Latn",
    "name:be-tarask",
    // https://wiki.openstreetmap.org/wiki/Multilingual_names#Japan
    "name:ja",
    "name:ja-Latn",
    "name:ja_rm",
    "name:ja_kana",
    // https://wiki.openstreetmap.org/wiki/Multilingual_names#China
    "name:zh-CN",
    "name:zh-hant-CN",
    "name:zh_pinyin",
    "name:zh_zhuyin",
    "name:zh-Latn-tongyong",
    "name:zh-Latn-pinyin",
    "name:zh-Latn-wadegiles",
    "name:yue-Latn-jyutping",
    // https://wiki.openstreetmap.org/wiki/Multilingual_names#France
    "name:fr",
    "name:fr-x-gallo",
    "name:br",
    "name:oc",
    "name:vls",
    "name:frp",
    "name:gcf",
    "name:gsw",
  })
  void testLatinFallbacks(String key) {
    if (key.startsWith("name:")) {
      assertTrue(LanguageUtils.isValidOsmNameTag(key));
    }
    assertEquals("a", LanguageUtils.getLatinName(Map.of(
      key, "a"
    ), true));
    assertNull(LanguageUtils.getLatinName(Map.of(
      key, "ア"
    ), true));
    assertNull(LanguageUtils.getLatinName(Map.of(
      key, "غ"
    ), true));
  }

  @ParameterizedTest
  @ValueSource(strings = {
    // OSM tags that should NOT be eligible for name:latin feature in the output
    "name:signed",
    "name:prefix",
    "name:abbreviation",
    "name:source",
    "name:full",
    "name:adjective",
    "name:proposed",
    "name:pronunciation",
    "name:etymology",
    "name:etymology:wikidata",
    "name:etymology:wikipedia",
    "name:etymology:right",
    "name:etymology:left",
    "name:genitive",
  })
  void testNoLatinFallback(String key) {
    assertFalse(LanguageUtils.isValidOsmNameTag(key));
    assertEquals("Branch Hill–Loveland Road", LanguageUtils.getLatinName(Map.of(
      "name", "Branch Hill–Loveland Road",
      key, "Q22133584;Q843993"
    ), true));
    assertEquals("rì", LanguageUtils.getLatinName(Map.of(
      "name", "日",
      key, "other"
    ), true));
  }

  @ParameterizedTest
  @CsvSource({
    "キャンパス, kyanpasu",
    "Αλφαβητικός Κατάλογος, Alphabētikós Katálogos",
    "биологическом, biologičeskom",
  })
  void testTransliterate(String in, String out) {
    assertEquals(out, LanguageUtils.getLatinName(Map.of(
      "name", in
    ), true));
    assertNull(LanguageUtils.getLatinName(Map.of(
      "name", in
    ), false));
  }

}
