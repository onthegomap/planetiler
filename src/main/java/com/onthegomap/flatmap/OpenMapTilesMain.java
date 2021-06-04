package com.onthegomap.flatmap;

import com.onthegomap.flatmap.collections.FeatureGroup;
import com.onthegomap.flatmap.collections.FeatureSort;
import com.onthegomap.flatmap.collections.LongLongMap;
import com.onthegomap.flatmap.profiles.OpenMapTilesProfile;
import com.onthegomap.flatmap.read.NaturalEarthReader;
import com.onthegomap.flatmap.read.OpenStreetMapReader;
import com.onthegomap.flatmap.read.OsmInputFile;
import com.onthegomap.flatmap.read.ShapefileReader;
import com.onthegomap.flatmap.write.MbtilesWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OpenMapTilesMain {

  private static final Logger LOGGER = LoggerFactory.getLogger(OpenMapTilesMain.class);

  public static void main(String[] args) throws IOException {
    Arguments arguments = Arguments.fromJvmProperties();
    var stats = arguments.getStats();
    var overallTimer = stats.startTimer("openmaptiles");
    LOGGER.info("Arguments:");
    Path sourcesDir = Path.of("data", "sources");
    OsmInputFile osmInputFile = new OsmInputFile(
      arguments.inputFile("input", "OSM input file", sourcesDir.resolve("north-america_us_massachusetts.pbf")));
    Path centerlines = arguments
      .inputFile("centerline", "lake centerlines input", sourcesDir.resolve("lake_centerline.shp.zip"));
    Path naturalEarth = arguments
      .inputFile("natural_earth", "natural earth input", sourcesDir.resolve("natural_earth_vector.sqlite.zip"));
    Path waterPolygons = arguments
      .inputFile("water_polygons", "water polygons input", sourcesDir.resolve("water-polygons-split-3857.zip"));
    Path tmpDir = arguments.file("tmpdir", "temp directory", Path.of("data", "tmp"));
    boolean fetchWikidata = arguments.get("fetch_wikidata", "fetch wikidata translations", false);
    boolean useWikidata = arguments.get("use_wikidata", "use wikidata translations", true);
    Path wikidataNamesFile = arguments.file("wikidata_cache", "wikidata cache file",
      Path.of("data", "sources", "wikidata_names.json"));
    Path output = arguments.file("output", "mbtiles output file", Path.of("data", "massachusetts.mbtiles"));
    List<String> languages = arguments.get("name_languages", "languages to use",
      "en,ru,ar,zh,ja,ko,fr,de,fi,pl,es,be,br,he".split(","));
    CommonParams config = CommonParams.from(arguments, osmInputFile);

    if (config.forceOverwrite()) {
      FileUtils.deleteFile(output);
    } else if (Files.exists(output)) {
      throw new IllegalArgumentException(output + " already exists, use force to overwrite.");
    }

    LOGGER.info("Building OpenMapTiles profile into " + output + " in these phases:");
    if (fetchWikidata) {
      LOGGER.info("  [wikidata] Fetch OpenStreetMap element name translations from wikidata");
    }
    LOGGER.info("  [lake_centerlines] Extract lake centerlines");
    LOGGER.info("  [water_polygons] Process ocean polygons");
    LOGGER.info("  [natural_earth] Process natural earth features");
    LOGGER.info("  [osm_pass1] Pre-process OpenStreetMap input (store node locations then relation members)");
    LOGGER.info("  [osm_pass2] Process OpenStreetMap nodes, ways, then relations");
    LOGGER.info("  [sort] Sort rendered features by tile ID");
    LOGGER.info("  [mbtiles] Encode each tile and write to " + output);

    var translations = Translations.defaultProvider(languages);
    var profile = new OpenMapTilesProfile();

    Files.createDirectories(tmpDir);
    Path nodeDb = tmpDir.resolve("node.db");
    LongLongMap nodeLocations = LongLongMap.newFileBackedSortedTable(nodeDb);
    FeatureSort featureDb = FeatureSort.newExternalMergeSort(tmpDir.resolve("feature.db"), config.threads(), stats);
    FeatureGroup featureMap = new FeatureGroup(featureDb, profile, stats);

    if (fetchWikidata) {
      stats.time("wikidata", () -> Wikidata.fetch(osmInputFile, wikidataNamesFile, config, profile, stats));
    }
    if (useWikidata) {
      translations.addTranslationProvider(Wikidata.load(wikidataNamesFile));
    }

    stats.time("lake_centerlines", () ->
      ShapefileReader
        .process("EPSG:3857", OpenMapTilesProfile.LAKE_CENTERLINE_SOURCE, centerlines, featureMap, config, profile,
          stats));
    stats.time("water_polygons", () ->
      ShapefileReader
        .process(OpenMapTilesProfile.WATER_POLYGON_SOURCE, waterPolygons, featureMap, config, profile, stats));
    stats.time("natural_earth", () ->
      NaturalEarthReader
        .process(OpenMapTilesProfile.NATURAL_EARTH_SOURCE, naturalEarth, tmpDir.resolve("natearth.sqlite"), featureMap,
          config, profile, stats)
    );

    try (var osmReader = new OpenStreetMapReader(OpenMapTilesProfile.OSM_SOURCE, osmInputFile, nodeLocations, profile,
      stats)) {
      stats.time("osm_pass1", () -> osmReader.pass1(config));
      stats.time("osm_pass2", () -> osmReader.pass2(featureMap, config));
    }

    LOGGER.info("Deleting node.db to make room for mbtiles");
    profile.release();
    Files.delete(nodeDb);

    stats.time("sort", featureDb::sort);

    stats.time("mbtiles", () -> MbtilesWriter.writeOutput(featureMap, output, profile, config, stats));

    overallTimer.stop();

    LOGGER.info("FINISHED!");

    stats.printSummary();
  }
}
