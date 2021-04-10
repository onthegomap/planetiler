package com.onthegomap.flatmap;

import com.graphhopper.reader.ReaderElement;
import com.onthegomap.flatmap.Wikidata.WikidataTranslations;
import java.util.List;
import java.util.Map;

public class Translations {

  public static Translations defaultProvider(List<String> languages) {
    // TODO
    return new Translations();
  }

  public void addTranslationProvider(WikidataTranslations load) {
    // TODO
  }


  public interface TranslationProvider {

    Map<String, String> getNameTranslations(ReaderElement elem);
  }
}
