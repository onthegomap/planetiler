package com.onthegomap.flatmap.profiles;


import com.graphhopper.util.StopWatch;
import com.onthegomap.flatmap.OsmInputFile;
import java.io.File;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OpenMapTilesProfile implements Profile {

  private static final Logger LOGGER = LoggerFactory.getLogger(OpenMapTilesProfile.class);

  public static void main(String[] args) {
    StopWatch watch = new StopWatch().start();
    Arguments arguments = new Arguments(args);
    OsmInputFile osmInputFile = new OsmInputFile(arguments.inputFile("input", "OSM input file",
      "./data/sources/massachusetts-latest.osm.pbf"));
    File centerlines = arguments.inputFile("centerline", "lake centerlines input",
      "./data/sources/lake_centerline.shp.zip");
    File naturalEarth = arguments.inputFile("natural_earth", "natural earth input",
      "./data/sources/natural_earth_vector.sqlite.zip");
    File waterPolygons = arguments.inputFile("water_polygons", "water polygons input",
      "./data/sources/water-polygons-split-3857.zip");
    double[] bounds = arguments.bounds("bounds", "bounds", osmInputFile);
    int threads = arguments.threads();
    File tmpDir = arguments.file("tmpdir", "temp directory", "./data/tmp");
    boolean fetchWikidata = arguments.get("fetch_wikidata", "fetch wikidata translations", false);
    boolean useWikidata = arguments.get("use_wikidata", "use wikidata translations", true);
    File wikidataNamesFile = arguments.file("wikidata_cache", "wikidata cache file",
      "./data/sources/wikidata_names.json");
    String output = arguments.get("output", "mbtiles output file", "./massachusetts.mbtiles");
    List<String> languages = arguments.get("name_languages", "languages to use",
      "en,ru,ar,zh,ja,ko,fr,de,fi,pl,es,be,br,he".split(","));

    var profile = new OpenMapTilesProfile();

    LOGGER.info("FINISHED! " + watch.stop());
  }
}
