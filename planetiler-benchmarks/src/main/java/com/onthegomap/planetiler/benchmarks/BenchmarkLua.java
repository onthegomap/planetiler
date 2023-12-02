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
    int batch = 1_000_000;
    var fc = new FeatureCollector.Factory(PlanetilerConfig.defaults(), Stats.inMemory());
    for (int i = 0; i < batch; i++) {
      env.profile.processFeature(feature, fc.get(feature));
    }
    long start = System.currentTimeMillis();
    int num = 0;
    do {
      for (int i = 0; i < batch; i++) {
        env.profile.processFeature(feature, fc.get(feature));
      }
      num += batch;
    } while (System.currentTimeMillis() - start < 1_000);
    long end = System.currentTimeMillis();
    System.err.println(Format.defaultInstance().numeric(num / ((end - start) / 1000.0)) + " calls/sec");
  }
}
