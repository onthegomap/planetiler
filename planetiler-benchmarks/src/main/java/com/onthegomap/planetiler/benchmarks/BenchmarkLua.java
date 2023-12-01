package com.onthegomap.planetiler.benchmarks;

import com.onthegomap.planetiler.FeatureCollector;
import com.onthegomap.planetiler.config.Arguments;
import com.onthegomap.planetiler.config.PlanetilerConfig;
import com.onthegomap.planetiler.experimental.lua.LuaEnvironment;
import com.onthegomap.planetiler.geo.GeoUtils;
import com.onthegomap.planetiler.reader.SimpleFeature;
import com.onthegomap.planetiler.stats.Stats;
import com.onthegomap.planetiler.util.Format;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.locationtech.jts.geom.CoordinateXY;

public class BenchmarkLua {

  public static void main(String[] args) throws IOException {
    var env =
      LuaEnvironment.loadScript(Arguments.of(), Path.of("planetiler-experimental/src/test/resources/power.lua"));
    var feature = SimpleFeature.createFakeOsmFeature(GeoUtils.JTS_FACTORY.createPoint(new CoordinateXY(0, 0)), Map.of(),
      "", "", 1, List.of());
    var fc = new FeatureCollector.Factory(PlanetilerConfig.defaults(), Stats.inMemory());
    for (int i = 0; i < 1_000; i++) {
      env.profile.processFeature(feature, fc.get(feature));
    }
    long start = System.currentTimeMillis();
    int num = 2_000_000;
    for (int i = 0; i < num; i++) {
      env.profile.processFeature(feature, fc.get(feature));
    }
    long end = System.currentTimeMillis();
    System.err.println("took " + (end - start) + "ms " +
      Format.defaultInstance().numeric(num / ((end - start) / 1000.0)) + " features/sec");
  }
}
