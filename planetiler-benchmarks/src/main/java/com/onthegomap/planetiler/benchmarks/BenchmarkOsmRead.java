package com.onthegomap.planetiler.benchmarks;

import com.onthegomap.planetiler.Profile;
import com.onthegomap.planetiler.collection.LongLongMap;
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
    OsmInputFile file = new OsmInputFile(Path.of("data/sources/northeast.osm.pbf"), true);
    var profile = new Profile.NullProfile();
    var stats = Stats.inMemory();
    var config = PlanetilerConfig.from(Arguments.of());

    while (true) {
      Timer timer = Timer.start();
      try (
        var nodes = LongLongMap.noop();
        var reader = new OsmReader("osm", file, nodes, profile, stats)
      ) {
        reader.pass1(config);
      }
      System.err.println(timer.stop());
    }
  }
}
