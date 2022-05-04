package com.onthegomap.planetiler.benchmarks;

import com.onthegomap.planetiler.basemap.BasemapProfile;
import com.onthegomap.planetiler.config.PlanetilerConfig;
import com.onthegomap.planetiler.expression.MultiExpression;
import com.onthegomap.planetiler.reader.SourceFeature;
import com.onthegomap.planetiler.reader.osm.OsmElement;
import com.onthegomap.planetiler.reader.osm.OsmInputFile;
import com.onthegomap.planetiler.stats.ProgressLoggers;
import com.onthegomap.planetiler.stats.Stats;
import com.onthegomap.planetiler.util.Translations;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.locationtech.jts.geom.Geometry;

/**
 * Performance tests for {@link MultiExpression}. Times how long a sample of elements from an OSM input file take to
 * match.
 */
public class BasemapMapping {

  public static void main(String[] args) {
    var profile = new BasemapProfile(Translations.nullProvider(List.of()), PlanetilerConfig.defaults(),
      Stats.inMemory());
    var random = new Random(0);
    List<SourceFeature> inputs = new ArrayList<>();
    var logger = ProgressLoggers.create()
      .addRateCounter("inputs", inputs::size)
      .addProcessStats();
    try (var reader = OsmInputFile.readFrom(Path.of("data", "sources", "massachusetts.osm.pbf"))) {
      reader.forEachBlock(block -> {
        for (var element : block.decodeElements()) {
          if (random.nextDouble() < 0.9) {
            if (inputs.size() % 1_000_000 == 0) {
              logger.log();
            }
            inputs.add(new SourceFeature(element.tags(), "", "", null, element.id()) {
              @Override
              public Geometry latLonGeometry() {
                return null;
              }

              @Override
              public Geometry worldGeometry() {
                return null;
              }

              @Override
              public boolean isPoint() {
                return element instanceof OsmElement.Node;
              }

              @Override
              public boolean canBePolygon() {
                return element instanceof OsmElement.Way || element instanceof OsmElement.Relation;
              }

              @Override
              public boolean canBeLine() {
                return element instanceof OsmElement.Way;
              }
            });
          }
        }
      });
    }

    logger.log();
    System.err.println("read " + inputs.size() + " elems");

    long startStart = System.nanoTime();
    long count = -1;
    while (true) {
      count++;
      long start = System.nanoTime();
      int i = 0;
      for (SourceFeature in : inputs) {
        i += profile.getTableMatches(in).size();
      }
      if (count == 0) {
        startStart = System.nanoTime();
        logger.log();
        System.err.println("finished warmup");
      } else {
        logger.log();
        System.err.println(
          "took:" + Duration.ofNanos(System.nanoTime() - start).toMillis() + "ms found:" + i + " avg:" + (Duration
            .ofNanos(System.nanoTime() - startStart).toMillis() / count) + "ms");
      }
    }
  }
}
