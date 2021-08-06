package com.onthegomap.flatmap.openmaptiles;

import com.onthegomap.flatmap.FlatMapRunner;
import com.onthegomap.flatmap.Translations;
import com.onthegomap.flatmap.Wikidata;
import com.onthegomap.flatmap.config.Arguments;
import com.onthegomap.flatmap.config.CommonParams;
import com.onthegomap.flatmap.openmaptiles.generated.OpenMapTilesSchema;
import com.onthegomap.flatmap.reader.osm.OsmInputFile;
import com.onthegomap.flatmap.stats.Stats;
import java.nio.file.Path;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OpenMapTilesMain {

  private static final Logger LOGGER = LoggerFactory.getLogger(OpenMapTilesMain.class);
  private static final String fallbackOsmFile = "north-america_us_massachusetts.pbf";
  private static final Path sourcesDir = Path.of("data", "sources");

  public static void main(String[] args) throws Exception {

    FlatMapRunner runner = FlatMapRunner.create();

    runner
      .setProfile(createProfileWithWikidataTranslations(runner))
      .addShapefileSource("EPSG:3857", OpenMapTilesProfile.LAKE_CENTERLINE_SOURCE,
        sourcesDir.resolve("lake_centerline.shp.zip"))
      .addShapefileSource(OpenMapTilesProfile.WATER_POLYGON_SOURCE,
        sourcesDir.resolve("water-polygons-split-3857.zip"))
      .addNaturalEarthSource(OpenMapTilesProfile.NATURAL_EARTH_SOURCE,
        sourcesDir.resolve("natural_earth_vector.sqlite.zip"))
      .addOsmSource(OpenMapTilesProfile.OSM_SOURCE, sourcesDir.resolve(fallbackOsmFile))
      .setOutput("mbtiles", Path.of("data", "massachusetts.mbtiles"))
      .run();
  }

  private static OpenMapTilesProfile createProfileWithWikidataTranslations(FlatMapRunner runner) throws Exception {
    Arguments arguments = runner.arguments();
    boolean fetchWikidata = arguments.get("fetch_wikidata", "fetch wikidata translations then continue", false);
    boolean onlyFetchWikidata = arguments.get("only_fetch_wikidata", "fetch wikidata translations then quit", false);
    boolean useWikidata = arguments.get("use_wikidata", "use wikidata translations", true);
    boolean transliterate = arguments.get("transliterate", "attempt to transliterate latin names", true);
    Path wikidataNamesFile = arguments.file("wikidata_cache", "wikidata cache file",
      Path.of("data", "sources", "wikidata_names.json"));
    // most common languages: "en,ru,ar,zh,ja,ko,fr,de,fi,pl,es,be,br,he"
    List<String> languages = arguments
      .get("name_languages", "languages to use", OpenMapTilesSchema.LANGUAGES.toArray(String[]::new));
    var translations = Translations.defaultProvider(languages).setShouldTransliterate(transliterate);
    var profile = new OpenMapTilesProfile(translations, arguments, runner.stats());

    if (onlyFetchWikidata) {
      LOGGER.info("Will fetch wikidata translations then quit...");
      var osmInput = new OsmInputFile(
        arguments.inputFile(OpenMapTilesProfile.OSM_SOURCE, "input file", sourcesDir.resolve(fallbackOsmFile)));
      Wikidata
        .fetch(osmInput, wikidataNamesFile, CommonParams.from(arguments, osmInput), profile, Stats.inMemory());
      translations.addTranslationProvider(Wikidata.load(wikidataNamesFile));
      System.exit(0);
    }

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
