package com.onthegomap.flatmap.examples;

import com.graphhopper.reader.ReaderRelation;
import com.onthegomap.flatmap.Arguments;
import com.onthegomap.flatmap.CommonParams;
import com.onthegomap.flatmap.FeatureCollector;
import com.onthegomap.flatmap.FeatureMerge;
import com.onthegomap.flatmap.FileUtils;
import com.onthegomap.flatmap.Profile;
import com.onthegomap.flatmap.SourceFeature;
import com.onthegomap.flatmap.VectorTileEncoder;
import com.onthegomap.flatmap.collections.FeatureGroup;
import com.onthegomap.flatmap.collections.FeatureSort;
import com.onthegomap.flatmap.collections.LongLongMap;
import com.onthegomap.flatmap.geo.GeometryException;
import com.onthegomap.flatmap.profiles.OpenMapTilesProfile;
import com.onthegomap.flatmap.read.OpenStreetMapReader;
import com.onthegomap.flatmap.read.OsmInputFile;
import com.onthegomap.flatmap.write.MbtilesWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BikeRouteOverlay implements Profile {

  private static final Logger LOGGER = LoggerFactory.getLogger(BikeRouteOverlay.class);

  private static record RouteRelationInfo(String name, String ref, String route, String network) implements
    OpenStreetMapReader.RelationInfo {}

  @Override
  public List<OpenStreetMapReader.RelationInfo> preprocessOsmRelation(ReaderRelation relation) {
    if (relation.hasTag("type", "route")) {
      String type = relation.getTag("route");
      if ("mtb".equals(type) || "bicycle".equals(type)) {
        return List.of(new RouteRelationInfo(
          relation.getTag("name"),
          relation.getTag("ref"),
          type,
          relation.getTag("network", "")
        ));
      }
    }
    return null;
  }

  @Override
  public void processFeature(SourceFeature sourceFeature, FeatureCollector features) {
    if (sourceFeature.canBeLine()) {
      for (RouteRelationInfo routeInfo : sourceFeature.relationInfo(RouteRelationInfo.class)) {
        int minzoom = switch (routeInfo.network) {
          case "icn", "ncn" -> 0;
          case "rcn" -> 10;
          default -> 12;
        };
        features.line("bikeroutes-" + routeInfo.route + "-" + routeInfo.network)
          .setAttr("name", routeInfo.name)
          .setAttr("ref", routeInfo.ref)
          .setZoomRange(minzoom, 14)
          .setMinPixelSize(0);
      }
    }
  }

  @Override
  public List<VectorTileEncoder.Feature> postProcessLayerFeatures(String layer, int zoom,
    List<VectorTileEncoder.Feature> items) throws GeometryException {
    return FeatureMerge.mergeLineStrings(items, 0.1, 0.1, 4);
  }

  @Override
  public String name() {
    return "Bike Paths Overlay";
  }

  @Override
  public String description() {
    return "An example overlay showing bicycle routes";
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
    Path mbtilesOutputPath = arguments.file("output", "mbtiles output file", Path.of("data", "bikeroutes.mbtiles"));
    CommonParams config = CommonParams.from(arguments, osmInputFile);

    FileUtils.deleteFile(mbtilesOutputPath);

    LOGGER.info("Building Bike path overlay example into " + mbtilesOutputPath);

    var profile = new BikeRouteOverlay();

    Files.createDirectories(tmpDir);
    Path nodeDbPath = tmpDir.resolve("node.db");
    LongLongMap nodeLocations = LongLongMap.newFileBackedSortedTable(nodeDbPath);
    Path featureDbPath = tmpDir.resolve("feature.db");
    FeatureSort featureDb = FeatureSort.newExternalMergeSort(featureDbPath, config.threads(), stats);
    FeatureGroup featureMap = new FeatureGroup(featureDb, profile, stats);

    try (var osmReader = new OpenStreetMapReader(OpenMapTilesProfile.OSM_SOURCE, osmInputFile, nodeLocations, profile,
      stats)) {
      stats.time("osm_pass1", () -> osmReader.pass1(config));
      stats.time("osm_pass2", () -> osmReader.pass2(featureMap, config));
    }

    stats.time("sort", featureDb::sort);
    stats.time("mbtiles", () -> MbtilesWriter.writeOutput(featureMap, mbtilesOutputPath, profile, config, stats));

    overallTimer.stop();

    LOGGER.info("FINISHED!");

    stats.printSummary();
    stats.close();
  }
}
