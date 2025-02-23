package com.onthegomap.planetiler.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class TranslationsTest {

  @Test
  void testNull() {
    var translations = Translations.nullProvider(List.of("en"));
    assertEquals(Map.of(), translations.getTranslations(Map.of("name:en", "name")));
  }

  @Test
  void testWildcard() {
    var translations = Translations.defaultProvider(List.of("*"));
    assertEquals(Map.of("name:en", "name"), translations.getTranslations(Map.of("name:en", "name")));
    assertEquals(Map.of("name:sr-Latn", "name"), translations.getTranslations(Map.of("name:sr-Latn", "name")));
    assertEquals(Map.of("name:zh-Hant-TW", "name"), translations.getTranslations(Map.of("name:zh-Hant-TW", "name")));
    assertEquals(Map.of(), translations.getTranslations(Map.of("name:left", "name")));
    assertEquals(Map.of(), translations.getTranslations(Map.of("name:etymology:wikidata", "name")));
  }

  @Test
  void testDefaultProvider() {
    var translations = Translations.defaultProvider(List.of("en"));
    assertEquals(Map.of("name:en", "name"), translations.getTranslations(Map.of("name:en", "name", "name:de", "de")));
  }

  @Test
  void testTwoProvidersPrefersFirst() {
    var translations = Translations.defaultProvider(List.of("en", "es", "de"))
      .addFallbackTranslationProvider(elem -> Map.of("name:de", "de2", "name:en", "en2"));
    assertEquals(Map.of("name:en", "en1", "name:es", "es1", "name:de", "de2"),
      translations.getTranslations(Map.of("name:en", "en1", "name:es", "es1")));
  }


  @Test
  void testTransliterate() {
    assertEquals("rì běn", Translations.transliterate("日本"));
  }

  @ParameterizedTest
  @MethodSource("includeExcludeCases")
  void testIncludeExclude(List<String> languages, List<String> shouldCare, List<String> shouldNotCare) {
    var translations = Translations.nullProvider(languages);
    for (var lang : shouldCare) {
      assertTrue(translations.careAboutLanguage(lang));
    }
    for (var lang : shouldNotCare) {
      assertFalse(translations.careAboutLanguage(lang));
    }
  }

  private static Stream<Arguments> includeExcludeCases() {
    return Stream.of(
      Arguments.of(List.of("jbo", "tlh"), List.of("jbo", "tlh"), List.of("en", "fr")),
      Arguments.of(List.of("*"), List.of("jbo", "tlh", "en", "fr"), List.of()),
      Arguments.of(List.of("*", "-tlh"), List.of("jbo", "fr"), List.of("tlh")),
      Arguments.of(List.of("tlh", "-tlh"), List.of(), List.of("tlh", "en"))
    );
  }


  @Test
  void testExtraNameTags() {
    Map<String, Object> input = Map.of(
      "extra", "extra",
      "name", "name",
      "name:en", "english name",
      "name:de", "german name"
    );
    var translations = Translations.defaultProvider(List.of("en", "es", "de"));
    Map<String, Object> output1 = new HashMap<String,Object>();
    translations.addTranslations(output1, input);
    assertFalse(output1.containsKey("extra"));

    Map<String, Object> output2 = new HashMap<String,Object>();
    translations.setExtraNameTags(List.of("extra"));
    translations.addTranslations(output2, input);
    assertTrue(output2.containsKey("extra"));
  }
}
