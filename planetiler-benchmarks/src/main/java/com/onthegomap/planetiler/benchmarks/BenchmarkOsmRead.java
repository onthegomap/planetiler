package com.onthegomap.planetiler.benchmarks;

import com.onthegomap.planetiler.Profile;
import com.onthegomap.planetiler.collection.LongLongMap;
import com.onthegomap.planetiler.collection.LongLongMultimap;
import com.onthegomap.planetiler.config.Arguments;
import com.onthegomap.planetiler.config.PlanetilerConfig;
import com.onthegomap.planetiler.reader.osm.OsmInputFile;
import com.onthegomap.planetiler.reader.osm.OsmReader;
import com.onthegomap.planetiler.stats.Stats;
import com.onthegomap.planetiler.stats.Timer;
import java.io.IOException;
import java.nio.file.Path;

public class BenchmarkOsmRead {

  public static void main(String[] args) throws IOException {
    var profile = new Profile.NullProfile();
    var stats = Stats.inMemory();
    var parsedArgs = Arguments.fromArgsOrConfigFile(args);
    var config = PlanetilerConfig.from(parsedArgs);
    var path = parsedArgs.inputFile("osm_path", "path to osm file", Path.of("data/sources/northeast.osm.pbf"));
    OsmInputFile file = new OsmInputFile(path, config.osmLazyReads());

    while (true) {
      Timer timer = Timer.start();
      try (
        var nodes = LongLongMap.noop();
        var multipolygons = LongLongMultimap.noop();
        var reader = new OsmReader("osm", file, nodes, multipolygons, profile, stats)
      ) {
        reader.pass1(config);
      }
      System.err.println(timer.stop());
    }
  }
}
