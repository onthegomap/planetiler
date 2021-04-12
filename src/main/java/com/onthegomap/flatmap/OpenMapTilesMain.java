package com.onthegomap.flatmap;

import com.onthegomap.flatmap.collections.LongLongMap;
import com.onthegomap.flatmap.collections.MergeSortFeatureMap;
import com.onthegomap.flatmap.profiles.OpenMapTilesProfile;
import com.onthegomap.flatmap.reader.NaturalEarthReader;
import com.onthegomap.flatmap.reader.OpenStreetMapReader;
import com.onthegomap.flatmap.reader.ShapefileReader;
import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import org.apache.commons.io.FileUtils;
import org.locationtech.jts.geom.Envelope;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OpenMapTilesMain {

  private static final Logger LOGGER = LoggerFactory.getLogger(OpenMapTilesMain.class);

  public static void main(String[] args) throws IOException {
    Arguments arguments = new Arguments(args);
    var stats = arguments.getStats();
    stats.startTimer("import");
    LOGGER.info("Arguments:");
    OsmInputFile osmInputFile = new OsmInputFile(
      arguments.inputFile("input", "OSM input file", "./data/sources/north-america_us_massachusetts.pbf"));
    File centerlines = arguments
      .inputFile("centerline", "lake centerlines input", "./data/sources/lake_centerline.shp.zip");
    File naturalEarth = arguments.inputFile("natural_earth", "natural earth input",
      "./data/sources/natural_earth_vector.sqlite.zip");
    File waterPolygons = arguments.inputFile("water_polygons", "water polygons input",
      "./data/sources/water-polygons-split-3857.zip");
    double[] bounds = arguments.bounds("bounds", "bounds", osmInputFile);
    Envelope envelope = new Envelope(bounds[0], bounds[2], bounds[1], bounds[3]);
    int threads = arguments.threads();
    int logIntervalSeconds = arguments.integer("loginterval", "seconds between logs", 10);
    Path tmpDir = arguments.file("tmpdir", "temp directory", "./data/tmp").toPath();
    boolean fetchWikidata = arguments.get("fetch_wikidata", "fetch wikidata translations", false);
    boolean useWikidata = arguments.get("use_wikidata", "use wikidata translations", true);
    File wikidataNamesFile = arguments.file("wikidata_cache", "wikidata cache file",
      "./data/sources/wikidata_names.json");
    File output = arguments.file("output", "mbtiles output file", "./massachusetts.mbtiles");
    List<String> languages = arguments.get("name_languages", "languages to use",
      "en,ru,ar,zh,ja,ko,fr,de,fi,pl,es,be,br,he".split(","));

    LOGGER.info("Building OpenMapTiles profile into " + output + " in these phases:");
    if (fetchWikidata) {
      LOGGER.info("- [wikidata] Fetch OpenStreetMap element name translations from wikidata");
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

    FileUtils.forceMkdir(tmpDir.toFile());
    File nodeDb = tmpDir.resolve("node.db").toFile();
    Path featureDb = tmpDir.resolve("feature.db");
    LongLongMap nodeLocations = new LongLongMap.MapdbSortedTable(nodeDb);
    MergeSortFeatureMap featureMap = new MergeSortFeatureMap(featureDb, stats);
    FeatureRenderer renderer = new FeatureRenderer(stats);
    FlatMapConfig config = new FlatMapConfig(profile, envelope, threads, stats, logIntervalSeconds);

    if (fetchWikidata) {
      stats.time("wikidata", () -> Wikidata.fetch(osmInputFile, wikidataNamesFile, config));
    }
    if (useWikidata) {
      translations.addTranslationProvider(Wikidata.load(wikidataNamesFile));
    }

    stats.time("lake_centerlines", () ->
      new ShapefileReader("EPSG:3857", centerlines, stats)
        .process("lake_centerlines", renderer, featureMap, config));
    stats.time("water_polygons", () ->
      new ShapefileReader(waterPolygons, stats)
        .process("water_polygons", renderer, featureMap, config)
    );
    stats.time("natural_earth", () ->
      new NaturalEarthReader(naturalEarth, tmpDir.resolve("natearth.sqlite").toFile(), stats)
        .process("natural_earth", renderer, featureMap, config)
    );

    try (var osmReader = new OpenStreetMapReader(osmInputFile, nodeLocations, stats)) {
      stats.time("osm_pass1", () -> osmReader.pass1(config));
      stats
        .time("osm_pass2", () -> osmReader.pass2(renderer, featureMap, Math.max(threads / 4, 1), threads - 1, config));
    }

    LOGGER.info("Deleting node.db to make room for mbtiles");
    profile.release();
    nodeDb.delete();

    stats.time("sort", featureMap::sort);
    stats.time("mbtiles", () -> MbtilesWriter.writeOutput(featureMap, output, config));

    stats.stopTimer("import");

    LOGGER.info("FINISHED!");

    stats.printSummary();
  }
}
