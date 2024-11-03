package com.onthegomap.planetiler.benchmarks;

import com.onthegomap.planetiler.util.Format;
import com.onthegomap.planetiler.util.LoopLineMerger;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.Function;
import org.locationtech.jts.geom.CoordinateXY;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.operation.linemerge.LineMerger;
import org.locationtech.jts.util.GeometricShapeFactory;

public class BenchmarkLoopLineMerge {

  public static void main(String[] args) {
    for (int i = 0; i < 10; i++) {
      System.err.println(
        " JTS line merger (/s):\t" +
          timeJts(10) + "\t" +
          timeJts(1_000) + "\t" +
          timeJts(10_000) + "\t" +
          timeJts(100_000));
      System.err.println(
        "loop line merger (/s):\t" +
          timeLoop(10) + "\t" +
          timeLoop(1_000) + "\t" +
          timeLoop(10_000) + "\t" +
          timeLoop(100_000));
      System.err.println();
    }
  }

  private static String timeLoop(int parts) {
    return time(parts, geom -> {
      var merger = new LoopLineMerger();
      merger.add(geom);
      return merger.getMergedLineStrings(0.1, 0.1);
    });
  }

  private static String timeJts(int parts) {
    return time(parts, geom -> {
      var merger = new LineMerger();
      merger.add(geom);
      return merger.getMergedLineStrings();
    });
  }

  private static String time(int parts, Function<Geometry, Collection<LineString>> fn) {
    Geometry multiLinestring = makeCircles(parts);
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

  private static Geometry makeCircles(int parts) {
    var factory = new GeometricShapeFactory();
    factory.setCentre(new CoordinateXY(0, 0));
    factory.setSize(2);
    factory.setNumPoints(parts);
    List<LineString> lines = new ArrayList<>();
    lines.add(factory.createArc(0, Math.PI * 2));
    factory.setCentre(new CoordinateXY(1, 0));
    lines.add(factory.createArc(0, Math.PI * 2));
    factory.setCentre(new CoordinateXY(1, 1));
    lines.add(factory.createArc(0, Math.PI * 2));
    factory.setCentre(new CoordinateXY(0, 1));
    lines.add(factory.createArc(0, Math.PI * 2));
    return new GeometryFactory().createMultiLineString(lines.toArray(LineString[]::new));
  }
}
