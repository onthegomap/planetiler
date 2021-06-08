package com.onthegomap.flatmap.examples;

import com.onthegomap.flatmap.Arguments;
import com.onthegomap.flatmap.CommonParams;
import com.onthegomap.flatmap.FeatureCollector;
import com.onthegomap.flatmap.FileUtils;
import com.onthegomap.flatmap.Profile;
import com.onthegomap.flatmap.SourceFeature;
import com.onthegomap.flatmap.collections.FeatureGroup;
import com.onthegomap.flatmap.collections.FeatureSort;
import com.onthegomap.flatmap.collections.LongLongMap;
import com.onthegomap.flatmap.profiles.OpenMapTilesProfile;
import com.onthegomap.flatmap.read.OpenStreetMapReader;
import com.onthegomap.flatmap.read.OsmInputFile;
import com.onthegomap.flatmap.write.MbtilesWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ToiletsOverlay implements Profile {

  private static final Logger LOGGER = LoggerFactory.getLogger(ToiletsOverlay.class);

  AtomicInteger toiletNumber = new AtomicInteger(0);

  @Override
  public void processFeature(SourceFeature sourceFeature, FeatureCollector features) {
    if (sourceFeature.hasTag("amenity", "toilets")) {
      features.centroid("toilets")
        .setZoomRange(0, 14)
        .setZorder(toiletNumber.incrementAndGet())
        .setLabelGridSizeAndLimit(12, 32, 4);
    }
  }

  @Override
  public String name() {
    return "Toilets Overlay";
  }

  @Override
  public String description() {
    return "An example overlay showing toilets";
  }

  @Override
  public boolean isOverlay() {
    return true;
  }

  @Override
  public String attribution() {
    return """
      <a href="https://www.openstreetmap.org/copyright" target="_blank">&copy; OpenStreetMap contributors</a>
      """.trim();
  }

  public static void main(String[] args) throws Exception {
    Arguments arguments = Arguments.fromJvmProperties();
    var stats = arguments.getStats();
    var overallTimer = stats.startTimer("overall");
    Path sourcesDir = Path.of("data", "sources");
    OsmInputFile osmInputFile = new OsmInputFile(
      arguments.inputFile("input", "OSM input file", sourcesDir.resolve("north-america_us_massachusetts.pbf")));
    Path tmpDir = arguments.file("tmpdir", "temp directory", Path.of("data", "tmp"));
    Path mbtilesOutputPath = arguments.file("output", "mbtiles output file", Path.of("data", "toilets.mbtiles"));
    CommonParams config = CommonParams.from(arguments, osmInputFile);

    FileUtils.deleteFile(mbtilesOutputPath);

    var profile = new ToiletsOverlay();

    Files.createDirectories(tmpDir);
    LongLongMap nodeLocations = LongLongMap.newFileBackedSortedTable(tmpDir.resolve("node.db"));
    FeatureSort featureDb = FeatureSort.newExternalMergeSort(tmpDir.resolve("feature.db"), config.threads(), stats);
    FeatureGroup featureMap = new FeatureGroup(featureDb, profile, stats);

    try (var osmReader = new OpenStreetMapReader(OpenMapTilesProfile.OSM_SOURCE, osmInputFile, nodeLocations, profile,
      stats)) {
      osmReader.pass1(config);
      osmReader.pass2(featureMap, config);
    }

    featureDb.sort();
    MbtilesWriter.writeOutput(featureMap, mbtilesOutputPath, profile, config, stats);

    overallTimer.stop();

    LOGGER.info("FINISHED!");

    stats.printSummary();
    stats.close();
  }
}
