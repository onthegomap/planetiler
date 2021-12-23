package com.onthegomap.planetiler.util;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

public class TranslationsTest {

  @Test
  public void testNull() {
    var translations = Translations.nullProvider(List.of("en"));
    assertEquals(Map.of(), translations.getTranslations(Map.of("name:en", "name")));
  }

  @Test
  public void testDefaultProvider() {
    var translations = Translations.defaultProvider(List.of("en"));
    assertEquals(Map.of("name:en", "name"), translations.getTranslations(Map.of("name:en", "name", "name:de", "de")));
  }

  @Test
  public void testTwoProviders() {
    var translations = Translations.defaultProvider(List.of("en", "es", "de"))
      .addTranslationProvider(elem -> Map.of("name:de", "de2", "name:en", "en2"));
    assertEquals(Map.of("name:en", "en2", "name:es", "es1", "name:de", "de2"),
      translations.getTranslations(Map.of("name:en", "en1", "name:es", "es1")));
  }
}
