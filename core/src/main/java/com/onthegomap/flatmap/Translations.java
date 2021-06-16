package com.onthegomap.flatmap;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class Translations {

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

  public static Translations nullProvider(List<String> languages) {
    return new Translations(languages);
  }

  public static Translations defaultProvider(List<String> languages) {
    return nullProvider(languages).addTranslationProvider(new OsmTranslationProvider());
  }

  public Translations addTranslationProvider(TranslationProvider provider) {
    providers.add(provider);
    return this;
  }

  public Map<String, Object> getTranslations(Map<String, Object> properties) {
    Map<String, Object> result = new HashMap<>();
    addTranslations(result, properties);
    return result;
  }

  public void addTranslations(Map<String, Object> result, Map<String, Object> properties) {
    for (TranslationProvider provider : providers) {
      Map<String, String> translations = provider.getNameTranslations(properties);
      if (translations != null && !translations.isEmpty()) {
        for (var entry : translations.entrySet()) {
          String key = entry.getKey();
          if (languageSet.contains(key)) {
            result.put(key.startsWith("name:") ? key : "name:" + key, entry.getValue());
          }
        }
      }
    }
  }

  public boolean getShouldTransliterate() {
    return shouldTransliterate;
  }

  public Translations setShouldTransliterate(boolean shouldTransliterate) {
    this.shouldTransliterate = shouldTransliterate;
    return this;
  }


  public interface TranslationProvider {

    Map<String, String> getNameTranslations(Map<String, Object> elem);
  }

  public static class OsmTranslationProvider implements TranslationProvider {

    @Override
    public Map<String, String> getNameTranslations(Map<String, Object> elem) {
      Map<String, String> result = new HashMap<>();
      for (String key : elem.keySet()) {
        if (key.startsWith("name:")) {
          Object value = elem.get(key);
          if (value instanceof String stringVal) {
            result.put(key, stringVal);
          }
        }
      }
      return result;
    }
  }
}
