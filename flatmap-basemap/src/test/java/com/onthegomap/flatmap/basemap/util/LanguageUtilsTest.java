package com.onthegomap.flatmap.basemap.util;

import static com.onthegomap.flatmap.TestUtils.assertSubmap;
import static com.onthegomap.flatmap.basemap.util.LanguageUtils.containsOnlyLatinCharacters;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;

import com.onthegomap.flatmap.util.Translations;
import com.onthegomap.flatmap.util.Wikidata;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

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
    "ɐ, false",
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
  @CsvSource({
    "name, a, true",
    "name:en, a, true",
    "int_name, a, true",
    "name:fr, a, true",
    "name:es, a, true",
    "name:pt, a, true",
    "name:de, a, true",
    "name:ar, ِغَّ, false",
    "name:it, a, true",
    "name:jp, ア, false",
    "name:jp-Latn, a, true",
    "name:jp_rm, a, true",
  })
  public void testLatinFallbacks(String key, String value, boolean use) {
    assertEquals(use ? value : null, LanguageUtils.getNames(Map.of(
      key, value
    ), translations).get("name:latin"));
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
