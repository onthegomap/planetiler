package com.onthegomap.planetiler.basemap.util;

import static com.onthegomap.planetiler.TestUtils.assertSubmap;
import static com.onthegomap.planetiler.basemap.util.LanguageUtils.containsOnlyLatinCharacters;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;

import com.onthegomap.planetiler.util.Translations;
import com.onthegomap.planetiler.util.Wikidata;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

public class LanguageUtilsTest {

  private final Wikidata.WikidataTranslations wikidataTranslations = new Wikidata.WikidataTranslations();
  private final Translations translations = Translations.defaultProvider(List.of("en", "es", "de"))
    .addTranslationProvider(wikidataTranslations);

  @Test
  public void testSimpleExample() {
    assertSubmap(Map.of(
      "name", "name",
      "name_en", "english name",
      "name_de", "german name"
    ), LanguageUtils.getNames(Map.of(
      "name", "name",
      "name:en", "english name",
      "name:de", "german name"
    ), translations));

    assertSubmap(Map.of(
      "name", "name",
      "name_en", "name",
      "name_de", "german name"
    ), LanguageUtils.getNames(Map.of(
      "name", "name",
      "name:de", "german name"
    ), translations));

    assertSubmap(Map.of(
      "name", "name",
      "name_en", "english name",
      "name_de", "name"
    ), LanguageUtils.getNames(Map.of(
      "name", "name",
      "name:en", "english name"
    ), translations));
  }

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
  public void testIsLatin(String in, boolean isLatin) {
    if (!isLatin) {
      assertFalse(containsOnlyLatinCharacters(in));
    } else {
      assertEquals(in, LanguageUtils.getNames(Map.of(
        "name", in
      ), translations).get("name:latin"));
    }
  }

  @ParameterizedTest
  @CsvSource(value = {
    "abcaāíìś+, null",
    "abca日āíìś+, 日+",
    "(abc), null",
    "日本 (Japan), 日本",
    "日本 [Japan - Nippon], 日本",
    "  Japan - Nippon (Japan) - Japan - 日本 - Japan - Nippon (Japan), 日本",
    "Japan - 日本~+  , 日本~+",
    "Japan / 日本 / Japan  , 日本",
  }, nullValues = "null")
  public void testRemoveNonLatin(String in, String out) {
    assertEquals(out, LanguageUtils.getNames(Map.of(
      "name", in
    ), translations).get("name:nonlatin"));
  }

  @ParameterizedTest
  @ValueSource(strings = {
    // OSM tags that SHOULD be eligible for name:latin feature in the output
    "name:en",
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
    "name:br",
    "name:oc",
    "name:vls",
    "name:frp",
    "name:gcf",
    "name:gsw",
  })
  public void testLatinFallbacks(String key) {
    assertEquals("a", LanguageUtils.getNames(Map.of(
      key, "a"
    ), translations).get("name:latin"));
    assertNull(LanguageUtils.getNames(Map.of(
      key, "ア"
    ), translations).get("name:latin"));
    assertNull(LanguageUtils.getNames(Map.of(
      key, "غ"
    ), translations).get("name:latin"));
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
  public void testNoLatinFallback(String key) {
    assertSubmap(Map.of(
      "name", "Branch Hill–Loveland Road",
      "name_en", "Branch Hill–Loveland Road",
      "name_de", "Branch Hill–Loveland Road",
      "name:latin", "Branch Hill–Loveland Road",
      "name_int", "Branch Hill–Loveland Road"
    ), LanguageUtils.getNames(Map.of(
      "name", "Branch Hill–Loveland Road",
      key, "Q22133584;Q843993"
    ), translations));
    assertSubmap(Map.of(
      "name", "日",
      "name_en", "日",
      "name_de", "日",
      "name:latin", "rì",
      "name_int", "rì"
    ), LanguageUtils.getNames(Map.of(
      "name", "日",
      key, "other" // don't use this latin string with invalid name keys
    ), translations));
  }

  @ParameterizedTest
  @CsvSource({
    "キャンパス, kyanpasu",
    "Αλφαβητικός Κατάλογος, Alphabētikós Katálogos",
    "биологическом, biologičeskom",
  })
  public void testTransliterate(String in, String out) {
    assertEquals(out, LanguageUtils.getNames(Map.of(
      "name", in
    ), translations).get("name:latin"));
    translations.setShouldTransliterate(false);
    assertNull(LanguageUtils.getNames(Map.of(
      "name", in
    ), translations).get("name:latin"));
  }

  @Test
  public void testUseWikidata() {
    wikidataTranslations.put(123, "es", "es name");
    assertSubmap(Map.of(
      "name:es", "es name"
    ), LanguageUtils.getNames(Map.of(
      "name", "name",
      "wikidata", "Q123"
    ), translations));
  }

  @Test
  public void testUseOsm() {
    assertSubmap(Map.of(
      "name:es", "es name osm"
    ), LanguageUtils.getNames(Map.of(
      "name", "name",
      "wikidata", "Q123",
      "name:es", "es name osm"
    ), translations));
  }

  @Test
  public void testPreferWikidata() {
    wikidataTranslations.put(123, "es", "wd es name");
    assertSubmap(Map.of(
      "name:es", "wd es name",
      "name:de", "de name osm"
    ), LanguageUtils.getNames(Map.of(
      "name", "name",
      "wikidata", "Q123",
      "name:es", "es name osm",
      "name:de", "de name osm"
    ), translations));
  }

  @Test
  public void testDontUseTranslationsWhenNotSpecified() {
    var result = LanguageUtils.getNamesWithoutTranslations(Map.of(
      "name", "name",
      "wikidata", "Q123",
      "name:es", "es name osm",
      "name:de", "de name osm"
    ));
    assertNull(result.get("name:es"));
    assertNull(result.get("name:de"));
    assertEquals("name", result.get("name"));
  }

  @Test
  public void testIgnoreLanguages() {
    wikidataTranslations.put(123, "ja", "ja name wd");
    var result = LanguageUtils.getNamesWithoutTranslations(Map.of(
      "name", "name",
      "wikidata", "Q123",
      "name:ja", "ja name osm"
    ));
    assertNull(result.get("name:ja"));
  }
}
