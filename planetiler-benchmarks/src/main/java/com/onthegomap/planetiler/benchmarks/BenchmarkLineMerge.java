package com.onthegomap.planetiler.benchmarks;

import com.onthegomap.planetiler.geo.GeoUtils;
import com.onthegomap.planetiler.util.Format;
import com.onthegomap.planetiler.util.LoopLineMerger;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Random;
import java.util.function.Function;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.CoordinateXY;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.operation.linemerge.LineMerger;

public class BenchmarkLineMerge {

  public static void main(String[] args) {
    for (int i = 0; i < 10; i++) {
      System.err.println(
        " JTS line merger (/s):\t" +
          timeJts(10, 10) + "\t" +
          timeJts(10, 100) + "\t" +
          timeJts(1000, 10) + "\t" +
          timeJts(1000, 1000));
      System.err.println(
        "loop line merger (/s):\t" +
          timeLoop(10, 10) + "\t" +
          timeLoop(10, 100) + "\t" +
          timeLoop(1000, 10) + "\t" +
          timeLoop(1000, 1000));
      System.err.println();
    }
  }

  private static String timeLoop(int lines, int parts) {
    return time(lines, parts, geom -> {
      var merger = new LoopLineMerger();
      merger.add(geom);
      merger.setLoopMinLength(0.1);
      merger.setMinLength(0.1);
      return merger.getMergedLineStrings();
    });
  }

  private static String timeJts(int lines, int parts) {
    return time(lines, parts, geom -> {
      var merger = new LineMerger();
      merger.add(geom);
      return merger.getMergedLineStrings();
    });
  }

  private static String time(int lines, int parts, Function<Geometry, Collection<LineString>> fn) {
    Geometry multiLinestring = makeLines(lines, parts);
    long start = System.nanoTime();
    long end = start + Duration.ofSeconds(1).toNanos();
    int num = 0;
    for (; System.nanoTime() < end;) {
      fn.apply(multiLinestring);
      num++;
    }
    return Format.defaultInstance()
      .numeric(Math.round(num * 1d / ((System.nanoTime() - start) * 1d / Duration.ofSeconds(1).toNanos())), true);
  }

  private static Geometry makeLines(int lines, int parts) {
    List<LineString> result = new ArrayList<>();
    var random = new Random(0);
    for (int i = 0; i < lines; i++) {
      Coordinate[] coords = new Coordinate[parts];
      double startX = random.nextInt(100);
      double startY = random.nextInt(100);
      double endX = random.nextInt(100);
      double endY = random.nextInt(100);
      for (int j = 0; j < parts; j++) {
        coords[j] = new CoordinateXY(
          j * (endX - startX) + startX,
          j * (endY - startY) + startY
        );
      }
      result.add(GeoUtils.JTS_FACTORY.createLineString(coords));
    }
    return new GeometryFactory().createMultiLineString(result.toArray(LineString[]::new));
  }
}
