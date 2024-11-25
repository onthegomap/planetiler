package com.onthegomap.planetiler.benchmarks;

import com.onthegomap.planetiler.geo.DouglasPeuckerSimplifier;
import com.onthegomap.planetiler.geo.VWSimplifier;
import com.onthegomap.planetiler.util.Format;
import com.onthegomap.planetiler.util.FunctionThatThrows;
import java.math.BigDecimal;
import java.math.MathContext;
import java.time.Duration;
import org.locationtech.jts.geom.CoordinateXY;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.util.GeometricShapeFactory;

public class BenchmarkSimplify {
  private static int numLines;

  public static void main(String[] args) throws Exception {
    for (int i = 0; i < 10; i++) {
      time("    DP(0.1)", geom -> DouglasPeuckerSimplifier.simplify(geom, 0.1));
      time("      DP(1)", geom -> DouglasPeuckerSimplifier.simplify(geom, 1));
      time("     DP(20)", geom -> DouglasPeuckerSimplifier.simplify(geom, 20));
      time("  JTS VW(0)", geom -> org.locationtech.jts.simplify.VWSimplifier.simplify(geom, 0.01));
      time("JTS VW(0.1)", geom -> org.locationtech.jts.simplify.VWSimplifier.simplify(geom, 0.1));
      time("  JTS VW(1)", geom -> org.locationtech.jts.simplify.VWSimplifier.simplify(geom, 1));
      time(" JTS VW(20)", geom -> org.locationtech.jts.simplify.VWSimplifier.simplify(geom, 20));
      time("      VW(0)", geom -> new VWSimplifier().setTolerance(0).setWeight(0.7).transform(geom));
      time("    VW(0.1)", geom -> new VWSimplifier().setTolerance(0.1).setWeight(0.7).transform(geom));
      time("      VW(1)", geom -> new VWSimplifier().setTolerance(1).setWeight(0.7).transform(geom));
      time("     VW(20)", geom -> new VWSimplifier().setTolerance(20).setWeight(0.7).transform(geom));
    }
    System.err.println(numLines);
  }

  private static void time(String name, FunctionThatThrows<Geometry, Geometry> fn) throws Exception {
    System.err.println(String.join("\t",
      name,
      timePerSec(makeLines(2), fn),
      timePerSec(makeLines(10), fn),
      timePerSec(makeLines(50), fn),
      timePerSec(makeLines(100), fn),
      timePerSec(makeLines(10_000), fn)
    ));
  }

  private static String timePerSec(Geometry geometry, FunctionThatThrows<Geometry, Geometry> fn)
    throws Exception {
    long start = System.nanoTime();
    long end = start + Duration.ofSeconds(1).toNanos();
    int num = 0;
    boolean first = true;
    for (; System.nanoTime() < end;) {
      numLines += fn.apply(geometry).getNumPoints();
      if (first) {
        first = false;
      }
      num++;
    }
    return Format.defaultInstance()
      .numeric(Math.round(num * 1d / ((System.nanoTime() - start) * 1d / Duration.ofSeconds(1).toNanos())), true);
  }

  private static String timeMillis(Geometry geometry, FunctionThatThrows<Geometry, Geometry> fn)
    throws Exception {
    long start = System.nanoTime();
    long end = start + Duration.ofSeconds(1).toNanos();
    int num = 0;
    for (; System.nanoTime() < end;) {
      numLines += fn.apply(geometry).getNumPoints();
      num++;
    }
    // equivalent of toPrecision(3)
    long nanosPer = (System.nanoTime() - start) / num;
    var bd = new BigDecimal(nanosPer, new MathContext(3));
    return Format.padRight(Duration.ofNanos(bd.longValue()).toString().replace("PT", ""), 6);
  }

  private static Geometry makeLines(int parts) {
    var shapeFactory = new GeometricShapeFactory();
    shapeFactory.setNumPoints(parts);
    shapeFactory.setCentre(new CoordinateXY(0, 0));
    shapeFactory.setSize(10);
    return shapeFactory.createCircle();
  }
}
