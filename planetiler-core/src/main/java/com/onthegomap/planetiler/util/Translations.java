package com.onthegomap.planetiler.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Holds planetiler configuration and utilities for translating element names to other languages.
 * <p>
 * {@link #defaultProvider(List)} filters {@code name:lang} tags from the input to a set of allowed output languages.
 * You can also add {@link Wikidata.WikidataTranslations} to use translated names from Wikidata items that the {@code
 * wikidata} tag on a source feature points to.
 */
public class Translations {
  private static final ThreadLocal<ThreadLocalTransliterator.TransliteratorInstance> TRANSLITERATOR =
    ThreadLocal.withInitial(() -> new ThreadLocalTransliterator().getInstance("Any-Latin"));

  private boolean shouldTransliterate = true;
  private final Set<String> languageSet;
  private final List<TranslationProvider> providers = new ArrayList<>();

  private Translations(List<String> languages) {
    this.languageSet = new HashSet<>();
    for (String language : languages) {
      String withoutPrefix = language.replaceFirst("^name:", "");
      languageSet.add(withoutPrefix);
      languageSet.add("name:" + withoutPrefix);
    }
  }

  /** Returns a new instance that does not have any translation providers. */
  public static Translations nullProvider(List<String> languages) {
    return new Translations(languages);
  }

  /**
   * Returns a new instance that extracts name translations from {@code name:lang} tags in input features.
   *
   * @param languages the set of 2-letter language codes to limit output translations to
   */
  public static Translations defaultProvider(List<String> languages) {
    return nullProvider(languages).addTranslationProvider(new OsmTranslationProvider());
  }

  /**
   * Mutates this translation instance to add {@code provider} which will be used only if all existing providers fail to
   * produce a translation for a given language.
   */
  public Translations addTranslationProvider(TranslationProvider provider) {
    providers.add(provider);
    return this;
  }

  /** Convenience wrapper for {@link #addTranslations(Map, Map)} that creates a new map for results. */
  public Map<String, Object> getTranslations(Map<String, Object> tags) {
    Map<String, Object> result = new HashMap<>();
    addTranslations(result, tags);
    return result;
  }

  /**
   * Gets name translations for {@code input} using all registered providers and puts the results into {@code output}
   * where {@code key=name:lang} and value is the translated name.
   */
  public void addTranslations(Map<String, Object> output, Map<String, Object> input) {
    for (TranslationProvider provider : providers) {
      Map<String, String> translations = provider.getNameTranslations(input);
      if (translations != null && !translations.isEmpty()) {
        for (var entry : translations.entrySet()) {
          String key = entry.getKey();
          if (languageSet.contains(key)) {
            output.put(key.startsWith("name:") ? key : "name:" + key, entry.getValue());
          }
        }
      }
    }
  }

  public boolean getShouldTransliterate() {
    return shouldTransliterate;
  }

  /** Store whether client should use expensive name transliteration. */
  public Translations setShouldTransliterate(boolean shouldTransliterate) {
    this.shouldTransliterate = shouldTransliterate;
    return this;
  }

  /** Returns true if {@code language} is in the set of language translations to use. */
  public boolean careAboutLanguage(String language) {
    return languageSet.contains(language);
  }

  /** A source of name translations. */
  public interface TranslationProvider {

    /**
     * Returns a map from language tag to translated name for a given input feature.
     *
     * @param tags key/value attributes for an input feature
     * @return the translated name
     */
    Map<String, String> getNameTranslations(Map<String, Object> tags);
  }

  /** Extracts translations from {@code name:lang} tags on an OSM element. */
  private static class OsmTranslationProvider implements TranslationProvider {

    @Override
    public Map<String, String> getNameTranslations(Map<String, Object> tags) {
      Map<String, String> result = new HashMap<>();
      for (var entry : tags.entrySet()) {
        String key = entry.getKey();
        if (key.startsWith("name:") && entry.getValue()instanceof String stringVal) {
          result.put(key, stringVal);
        }
      }
      return result;
    }
  }

  /**
   * Attempts to translate non-latin characters to latin characters that preserve the <em>sound</em> of the word (as
   * opposed to translation which attempts to preserve meaning) using ICU4j.
   * <p>
   * NOTE: This can be expensive and the quality is hit or miss, so exhaust all other options first.
   */
  public static String transliterate(String input) {
    return input == null ? null : TRANSLITERATOR.get().transliterate(input);
  }
}

