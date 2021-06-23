package com.onthegomap.flatmap.openmaptiles;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.graphhopper.reader.ReaderNode;
import com.onthegomap.flatmap.Arguments;
import com.onthegomap.flatmap.Translations;
import com.onthegomap.flatmap.Wikidata;
import com.onthegomap.flatmap.monitoring.Stats;
import java.util.List;
import org.junit.jupiter.api.Test;

public class OpenMaptilesProfileTest {

  private final Wikidata.WikidataTranslations wikidataTranslations = new Wikidata.WikidataTranslations();
  private final Translations translations = Translations.defaultProvider(List.of("en", "es", "de"))
    .addTranslationProvider(wikidataTranslations);
  private final OpenMapTilesProfile profile = new OpenMapTilesProfile(translations, Arguments.of(),
    new Stats.InMemory());

  @Test
  public void testCaresAboutWikidata() {
    var node = new ReaderNode(1, 1, 1);
    node.setTag("aeroway", "gate");
    assertTrue(profile.caresAboutWikidataTranslation(node));

    node.setTag("aeroway", "other");
    assertFalse(profile.caresAboutWikidataTranslation(node));
  }
}
