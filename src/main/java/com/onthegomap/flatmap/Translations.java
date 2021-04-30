package com.onthegomap.flatmap;

import com.graphhopper.reader.ReaderElement;
import java.util.List;
import java.util.Map;

public class Translations {

  public static Translations defaultProvider(List<String> languages) {
    // TODO
    return new Translations();
  }

  public static Translations defaultTranslationProvider() {
    return null;
  }

  public void addTranslationProvider(Wikidata.WikidataTranslations load) {
    // TODO
  }


  public interface TranslationProvider {

    Map<String, String> getNameTranslations(ReaderElement elem);
  }
}
