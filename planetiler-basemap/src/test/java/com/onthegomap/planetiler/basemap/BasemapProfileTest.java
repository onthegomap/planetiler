package com.onthegomap.planetiler.basemap;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.onthegomap.planetiler.config.PlanetilerConfig;
import com.onthegomap.planetiler.reader.osm.OsmElement;
import com.onthegomap.planetiler.stats.Stats;
import com.onthegomap.planetiler.util.Translations;
import com.onthegomap.planetiler.util.Wikidata;
import java.util.List;
import org.junit.jupiter.api.Test;

public class BasemapProfileTest {

  private final Wikidata.WikidataTranslations wikidataTranslations = new Wikidata.WikidataTranslations();
  private final Translations translations = Translations.defaultProvider(List.of("en", "es", "de"))
    .addTranslationProvider(wikidataTranslations);
  private final BasemapProfile profile = new BasemapProfile(translations, PlanetilerConfig.defaults(),
    Stats.inMemory());

  @Test
  public void testCaresAboutWikidata() {
    var node = new OsmElement.Node(1, 1, 1);
    node.setTag("aeroway", "gate");
    assertTrue(profile.caresAboutWikidataTranslation(node));

    node.setTag("aeroway", "other");
    assertFalse(profile.caresAboutWikidataTranslation(node));
  }

  @Test
  public void testDoesntCareAboutWikidataForRoads() {
    var way = new OsmElement.Way(1);
    way.setTag("highway", "footway");
    assertFalse(profile.caresAboutWikidataTranslation(way));
  }
}
