package com.onthegomap.flatmap;

import com.graphhopper.reader.ReaderElement;
import java.io.File;
import java.util.Map;

public class Wikidata {

  public static void fetch(OsmInputFile infile, File outfile, FlatMapConfig config) {
    // TODO
  }

  public static WikidataTranslations load(File namesFile) {
    // TODO
    return null;
  }

  public static class WikidataTranslations implements Translations.TranslationProvider {

    @Override
    public Map<String, String> getNameTranslations(ReaderElement elem) {
      // TODO
      return null;
    }
  }
}
