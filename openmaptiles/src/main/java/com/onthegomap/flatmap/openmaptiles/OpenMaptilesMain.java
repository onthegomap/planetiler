package com.onthegomap.flatmap.openmaptiles;

import com.onthegomap.flatmap.Arguments;
import com.onthegomap.flatmap.FlatMapRunner;
import com.onthegomap.flatmap.Translations;
import com.onthegomap.flatmap.Wikidata;
import java.nio.file.Path;
import java.util.List;

public class OpenMaptilesMain {

  public static void main(String[] args) throws Exception {
    Path sourcesDir = Path.of("data", "sources");

    FlatMapRunner runner = FlatMapRunner.create();

    runner
      .setProfile(createProfileWithWikidataTranslations(runner))
      .addShapefileSource("EPSG:3857", OpenMapTilesProfile.LAKE_CENTERLINE_SOURCE,
        sourcesDir.resolve("lake_centerline.shp.zip"))
      .addShapefileSource(OpenMapTilesProfile.WATER_POLYGON_SOURCE,
        sourcesDir.resolve("water-polygons-split-3857.zip"))
      .addNaturalEarthSource(OpenMapTilesProfile.NATURAL_EARTH_SOURCE,
        sourcesDir.resolve("natural_earth_vector.sqlite.zip"))
      .addOsmSource(OpenMapTilesProfile.OSM_SOURCE, sourcesDir.resolve("north-america_us_massachusetts.pbf"))
      .setOutput("mbtiles", Path.of("data", "massachusetts.mbtiles"))
      .run();
  }

  private static OpenMapTilesProfile createProfileWithWikidataTranslations(FlatMapRunner runner) {
    Arguments arguments = runner.arguments();
    boolean fetchWikidata = arguments.get("fetch_wikidata", "fetch wikidata translations", false);
    boolean useWikidata = arguments.get("use_wikidata", "use wikidata translations", true);
    Path wikidataNamesFile = arguments.file("wikidata_cache", "wikidata cache file",
      Path.of("data", "sources", "wikidata_names.json"));
    List<String> languages = arguments.get("name_languages", "languages to use",
      "en,ru,ar,zh,ja,ko,fr,de,fi,pl,es,be,br,he".split(","));
    var translations = Translations.defaultProvider(languages);
    var profile = new OpenMapTilesProfile(translations);

    if (useWikidata) {
      if (fetchWikidata) {
        runner.addStage("wikidata", "fetch translations from wikidata query service", () -> {
          Wikidata.fetch(runner.osmInputFile(), wikidataNamesFile, runner.config(), profile, runner.stats());
          translations.addTranslationProvider(Wikidata.load(wikidataNamesFile));
        });
      } else {
        translations.addTranslationProvider(Wikidata.load(wikidataNamesFile));
      }
    }
    return profile;
  }
}
