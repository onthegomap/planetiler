package com.onthegomap.planetiler.benchmarks;

import com.graphhopper.reader.ReaderElementUtils;
import com.graphhopper.reader.ReaderNode;
import com.graphhopper.reader.ReaderRelation;
import com.graphhopper.reader.ReaderWay;
import com.onthegomap.planetiler.basemap.BasemapProfile;
import com.onthegomap.planetiler.config.PlanetilerConfig;
import com.onthegomap.planetiler.expression.MultiExpression;
import com.onthegomap.planetiler.reader.SourceFeature;
import com.onthegomap.planetiler.reader.osm.OsmInputFile;
import com.onthegomap.planetiler.stats.Stats;
import com.onthegomap.planetiler.util.Translations;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.locationtech.jts.geom.Geometry;

/**
 * Performance tests for {@link MultiExpression}.  Times how long a sample of elements from an OSM input file take to
 * match.
 */
public class BasemapMapping {

  public static void main(String[] args) throws IOException {
    var profile = new BasemapProfile(Translations.nullProvider(List.of()), PlanetilerConfig.defaults(),
      Stats.inMemory());
    var random = new Random(0);
    var input = new OsmInputFile(Path.of("data", "sources", "north-america_us_massachusetts.pbf"));
    List<SourceFeature> inputs = new ArrayList<>();
    input.readTo(readerElem -> {
      if (random.nextDouble() < 0.2) {
        if (inputs.size() % 1_000_000 == 0) {
          System.err.println(inputs.size());
        }
        var props = ReaderElementUtils.getTags(readerElem);
        inputs.add(new SourceFeature(props, "", "", null, readerElem.getId()) {
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
            return readerElem instanceof ReaderNode;
          }

          @Override
          public boolean canBePolygon() {
            return readerElem instanceof ReaderWay || readerElem instanceof ReaderRelation;
          }

          @Override
          public boolean canBeLine() {
            return readerElem instanceof ReaderWay;
          }
        });
      }
    }, "reader", 3);

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
        System.err.println("finished warmup");
      } else {
        System.err.println(
          "took:" + Duration.ofNanos(System.nanoTime() - start).toMillis() + "ms found:" + i + " avg:" + (Duration
            .ofNanos(System.nanoTime() - startStart).toMillis() / count) + "ms");
      }
    }
  }
}
