package com.onthegomap.flatmap.openmaptiles;

import com.graphhopper.reader.ReaderElementUtils;
import com.graphhopper.reader.ReaderNode;
import com.graphhopper.reader.ReaderRelation;
import com.graphhopper.reader.ReaderWay;
import com.onthegomap.flatmap.SourceFeature;
import com.onthegomap.flatmap.geo.GeometryException;
import com.onthegomap.flatmap.openmaptiles.generated.Tables;
import com.onthegomap.flatmap.read.OsmInputFile;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.locationtech.jts.geom.Geometry;

public class BenchmarkMapping {

  public static void main(String[] args) throws IOException {
    var random = new Random(0);
    var input = new OsmInputFile(Path.of("data", "sources", "north-america_us_massachusetts.pbf"));
    List<SourceFeature> inputs = new ArrayList<>();
    input.readTo(readerElem -> {
      if (random.nextDouble() < 0.25) {
        if (inputs.size() % 1_000_000 == 0) {
          System.err.println(inputs.size());
        }
        var props = ReaderElementUtils.getProperties(readerElem);
        inputs.add(new SourceFeature(props, "", "", null, readerElem.getId()) {
          @Override
          public Geometry latLonGeometry() throws GeometryException {
            return null;
          }

          @Override
          public Geometry worldGeometry() throws GeometryException {
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
    var mappings = Tables.MAPPINGS.index();

    System.err.println("read " + inputs.size() + " elems");

    long startStart = System.nanoTime();
    long count = -1;
    while (true) {
      count++;
      long start = System.nanoTime();
      int i = 0;
      for (SourceFeature in : inputs) {
        OpenMapTilesProfile.MapWithType wrapped = new OpenMapTilesProfile.MapWithType(in);
        i += mappings.getMatchesWithTriggers(wrapped).size();
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
